import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import mysql from 'mysql2/promise';
import { createClient } from 'redis';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import cors from 'cors';
import path from 'path';
import 'dotenv/config';

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(process.cwd(), 'public')));

const JWT_SECRET = process.env.JWT_SECRET || 'Community360_Secreto_2026';

// Pool de conexiones a TiDB Cloud (SQL) con Keep-Alive
const pool = mysql.createPool({
  host: process.env.TIDB_HOST,
  port: Number(process.env.TIDB_PORT),
  user: process.env.TIDB_USER,
  password: process.env.TIDB_PASSWORD,
  database: process.env.TIDB_DATABASE,
  ssl: { minVersion: 'TLSv1.2', rejectUnauthorized: true },
  waitForConnections: true,
  connectionLimit: 10,
  enableKeepAlive: true,
  keepAliveInitialDelay: 10000
});

// Cliente Redis Cloud (NoSQL) con reconexión automática
const redis = createClient({
  url: process.env.REDIS_URL,
  socket: {
    reconnectStrategy: (retries) => Math.min(retries * 200, 3000)
  }
});
redis.on('error', (err) => console.error('Aviso Redis:', err.message));
await redis.connect();

// Helper: Obtener o crear ID en tablas catálogo (canales, temas, dispositivos)
async function getOrCreateCatalogId(conn, tabla, colId, nombre) {
  const [rows] = await conn.execute(`SELECT ${colId} AS id FROM ${tabla} WHERE nombre = ?`, [nombre]);
  if (rows.length > 0) return rows[0].id;
  const [res] = await conn.execute(`INSERT INTO ${tabla} (nombre) VALUES (?)`, [nombre]);
  return res.insertId;
}

// Helper: Obtener todos los KPIs desde Redis (con reconstrucción automática si está vacío)
async function obtenerKPIs() {
  let resumen = await redis.hGetAll('kpi:resumen');
  if (!resumen || Object.keys(resumen).length === 0) {
    const [[uRow]] = await pool.execute('SELECT COUNT(*) AS total FROM usuarios');
    const [[iRow]] = await pool.execute('SELECT COUNT(*) AS total, COALESCE(SUM(sentimiento),0) AS suma FROM interacciones');
    const [[qRow]] = await pool.execute('SELECT COUNT(*) AS total FROM cuarentena_interacciones');
    resumen = {
      total_usuarios: String(uRow.total),
      total_interacciones: String(iRow.total),
      total_cuarentena: String(qRow.total),
      suma_sentimiento: Number(iRow.suma).toFixed(4)
    };
    await redis.hSet('kpi:resumen', resumen);
  }

  const temas = await redis.hGetAll('kpi:temas');
  const canales = await redis.hGetAll('kpi:canales');
  const dispositivos = await redis.hGetAll('kpi:dispositivos');

  const totalInter = Number(resumen.total_interacciones) || 0;
  const sumaSent = Number(resumen.suma_sentimiento) || 0;
  const sentimientoPromedio = totalInter > 0 ? Number((sumaSent / totalInter).toFixed(3)) : 0;

  return {
    resumen: {
      total_usuarios: Number(resumen.total_usuarios) || 0,
      total_interacciones: totalInter,
      total_cuarentena: Number(resumen.total_cuarentena) || 0,
      suma_sentimiento: sumaSent,
      sentimiento_promedio: sentimientoPromedio
    },
    temas,
    canales,
    dispositivos
  };
}

// ============================================================================
// FASE 3: AUTENTICACIÓN Y MIDDLEWARE JWT
// ============================================================================
app.post('/api/login', async (req, res) => {
  try {
    const { username, password } = req.body;
    if (!username || !password) {
      return res.status(400).json({ error: 'Usuario y contraseña requeridos' });
    }

    const [rows] = await pool.execute('SELECT * FROM usuarios_sistema WHERE username = ?', [username.trim()]);
    if (rows.length === 0) {
      return res.status(401).json({ error: 'Credenciales inválidas' });
    }

    const user = rows[0];
    const valid = await bcrypt.compare(password, user.password_hash);
    if (!valid) {
      return res.status(401).json({ error: 'Credenciales inválidas' });
    }

    const token = jwt.sign({ id: user.id, username: user.username, rol: user.rol }, JWT_SECRET, { expiresIn: '4h' });
    res.json({ mensaje: 'Autenticación exitosa', token, usuario: { username: user.username, rol: user.rol } });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Error interno en autenticación' });
  }
});

function verificarToken(req, res, next) {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'Acceso denegado: token no proporcionado' });

  jwt.verify(token, JWT_SECRET, (err, decoded) => {
    if (err) return res.status(403).json({ error: 'Token inválido o expirado' });
    req.user = decoded;
    next();
  });
}

// ============================================================================
// FASE 4: ENDPOINTS REST (PROTEGIDOS CON JWT)
// ============================================================================

// 1. Indicadores en tiempo real desde Redis Cloud
app.get('/api/kpis', verificarToken, async (req, res) => {
  try {
    const kpis = await obtenerKPIs();
    res.json(kpis);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Error al consultar KPIs en Redis' });
  }
});

// 2. Consultas históricas desde TiDB Cloud (SQL)
app.get('/api/historicos', verificarToken, async (req, res) => {
  try {
    const [porFecha] = await pool.execute(`
      SELECT DATE_FORMAT(timestamp, '%Y-%m-%d') AS fecha, COUNT(*) AS total, ROUND(AVG(sentimiento), 3) AS sentimiento_medio
      FROM interacciones
      GROUP BY fecha
      ORDER BY fecha ASC
    `);

    const [porPais] = await pool.execute(`
      SELECT ub.pais, COUNT(*) AS total
      FROM usuario_ubicaciones ub
      JOIN usuarios u ON u.id_usuario = ub.id_usuario
      WHERE u.consentimiento_datos = TRUE AND ub.pais IS NOT NULL
      GROUP BY ub.pais
      ORDER BY total DESC
    `);

    const [porEspecie] = await pool.execute(`
      SELECT especie_fursona, COUNT(*) AS total
      FROM usuarios
      GROUP BY especie_fursona
      ORDER BY total DESC
      LIMIT 8
    `);

    const [porEstatus] = await pool.execute(`
      SELECT estatus, COUNT(*) AS total
      FROM usuarios
      GROUP BY estatus
    `);

    // Matriz para Mapa de Calor: Interacciones por Canal vs. Tema
    const [mapaCalor] = await pool.execute(`
      SELECT c.nombre AS canal, t.nombre AS tema, COUNT(*) AS total
      FROM interacciones i
      JOIN canales c ON i.id_canal = c.id_canal
      JOIN temas t ON i.id_tema = t.id_tema
      GROUP BY c.nombre, t.nombre
    `);

    res.json({ porFecha, porPais, porEspecie, porEstatus, mapaCalor });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Error al consultar datos históricos en TiDB' });
  }
});

// 3. Listar últimas interacciones válidas (JOIN con catálogos)
app.get('/api/interacciones', verificarToken, async (req, res) => {
  try {
    const [rows] = await pool.execute(`
      SELECT i.id_interaccion, i.id_usuario, u.alias, c.nombre AS canal, t.nombre AS tema,
             d.nombre AS dispositivo, i.timestamp, i.tipo, i.texto, i.sentimiento, i.reacciones_recibidas
      FROM interacciones i
      JOIN usuarios u ON i.id_usuario = u.id_usuario
      JOIN canales c ON i.id_canal = c.id_canal
      JOIN temas t ON i.id_tema = t.id_tema
      JOIN dispositivos d ON i.id_dispositivo = d.id_dispositivo
      ORDER BY i.timestamp DESC
      LIMIT 20
    `);
    res.json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Error al obtener interacciones' });
  }
});

// 4. POST: Crear nueva interacción (desde Postman) -> TiDB + Redis + WebSocket
app.post('/api/interacciones', verificarToken, async (req, res) => {
  const conn = await pool.getConnection();
  try {
    const {
      id_interaccion, id_usuario, canal, tema, dispositivo,
      tipo = 'publicacion', texto = null, sentimiento = 0, idioma = 'es', reacciones_recibidas = 0
    } = req.body;

    if (!id_interaccion || !id_usuario || !canal || !tema || !dispositivo) {
      return res.status(400).json({ error: 'Faltan campos obligatorios: id_interaccion, id_usuario, canal, tema, dispositivo' });
    }

    const sentNum = Number(sentimiento);
    if (Number.isNaN(sentNum) || sentNum < -1 || sentNum > 1) {
      return res.status(400).json({ error: 'El sentimiento debe ser un número entre -1.00 y 1.00' });
    }

    const [userCheck] = await conn.execute('SELECT id_usuario FROM usuarios WHERE id_usuario = ?', [id_usuario.trim()]);
    if (userCheck.length === 0) {
      return res.status(400).json({ error: `Integridad referencial: el usuario ${id_usuario} no existe en el padrón` });
    }

    await conn.beginTransaction();

    const idCanal = await getOrCreateCatalogId(conn, 'canales', 'id_canal', canal.trim());
    const idTema = await getOrCreateCatalogId(conn, 'temas', 'id_tema', tema.trim());
    const idDispositivo = await getOrCreateCatalogId(conn, 'dispositivos', 'id_dispositivo', dispositivo.trim());
    const longitud = texto ? texto.trim().length : 0;

    await conn.execute(
      `INSERT INTO interacciones (id_interaccion, id_usuario, id_canal, id_tema, id_dispositivo, timestamp, tipo, texto, sentimiento, idioma, longitud_texto, reacciones_recibidas)
       VALUES (?, ?, ?, ?, ?, NOW(), ?, ?, ?, ?, ?, ?)`,
      [id_interaccion.trim(), id_usuario.trim(), idCanal, idTema, idDispositivo, tipo.trim(), texto, sentNum, idioma.trim(), longitud, Number(reacciones_recibidas)]
    );

    await conn.commit();

    // Sincronización Write-Through en Redis Cloud
    await redis.hIncrBy('kpi:resumen', 'total_interacciones', 1);
    await redis.hIncrByFloat('kpi:resumen', 'suma_sentimiento', sentNum);
    await redis.hIncrBy('kpi:temas', tema.trim(), 1);
    await redis.hIncrBy('kpi:canales', canal.trim(), 1);
    await redis.hIncrBy('kpi:dispositivos', dispositivo.trim(), 1);

    // Emitir actualización en tiempo real por WebSocket
    const kpisActualizados = await obtenerKPIs();
    io.emit('actualizacion_kpis', { accion: 'POST', id_interaccion, kpis: kpisActualizados });

    res.status(201).json({ mensaje: 'Interacción creada y sincronizada en tiempo real', id_interaccion, kpis: kpisActualizados });
  } catch (err) {
    await conn.rollback();
    if (err.code === 'ER_DUP_ENTRY') {
      return res.status(409).json({ error: 'Ya existe una interacción con ese id_interaccion' });
    }
    console.error('Detalle error POST:', err);
    res.status(500).json({ error: 'Error al registrar la interacción', detalle: err.sqlMessage || err.message });
  } finally {
    conn.release();
  }
});

// 5. PUT: Modificar interacción existente (desde Postman) -> TiDB + Redis + WebSocket
app.put('/api/interacciones/:id', verificarToken, async (req, res) => {
  const conn = await pool.getConnection();
  try {
    const { id } = req.params;
    const { tema, canal, dispositivo, sentimiento, texto } = req.body;

    const [prevRows] = await conn.execute(`
      SELECT i.*, c.nombre AS canal_actual, t.nombre AS tema_actual, d.nombre AS disp_actual
      FROM interacciones i
      JOIN canales c ON i.id_canal = c.id_canal
      JOIN temas t ON i.id_tema = t.id_tema
      JOIN dispositivos d ON i.id_dispositivo = d.id_dispositivo
      WHERE i.id_interaccion = ?
    `, [id]);

    if (prevRows.length === 0) {
      return res.status(404).json({ error: `Interacción ${id} no encontrada` });
    }

    const prev = prevRows[0];
    const nuevoTema = tema ? tema.trim() : prev.tema_actual;
    const nuevoCanal = canal ? canal.trim() : prev.canal_actual;
    const nuevoDisp = dispositivo ? dispositivo.trim() : prev.disp_actual;
    const nuevoSent = sentimiento !== undefined ? Number(sentimiento) : Number(prev.sentimiento);
    const nuevoTexto = texto !== undefined ? texto : prev.texto;

    if (Number.isNaN(nuevoSent) || nuevoSent < -1 || nuevoSent > 1) {
      return res.status(400).json({ error: 'El sentimiento debe estar entre -1.00 y 1.00' });
    }

    await conn.beginTransaction();

    const idTema = await getOrCreateCatalogId(conn, 'temas', 'id_tema', nuevoTema);
    const idCanal = await getOrCreateCatalogId(conn, 'canales', 'id_canal', nuevoCanal);
    const idDisp = await getOrCreateCatalogId(conn, 'dispositivos', 'id_dispositivo', nuevoDisp);

    await conn.execute(
      `UPDATE interacciones SET id_tema = ?, id_canal = ?, id_dispositivo = ?, sentimiento = ?, texto = ?, longitud_texto = ?
       WHERE id_interaccion = ?`,
      [idTema, idCanal, idDisp, nuevoSent, nuevoTexto, nuevoTexto ? nuevoTexto.length : 0, id]
    );

    await conn.commit();

    const diffSent = nuevoSent - Number(prev.sentimiento);
    if (diffSent !== 0) await redis.hIncrByFloat('kpi:resumen', 'suma_sentimiento', diffSent);

    if (nuevoTema !== prev.tema_actual) {
      await redis.hIncrBy('kpi:temas', prev.tema_actual, -1);
      await redis.hIncrBy('kpi:temas', nuevoTema, 1);
    }
    if (nuevoCanal !== prev.canal_actual) {
      await redis.hIncrBy('kpi:canales', prev.canal_actual, -1);
      await redis.hIncrBy('kpi:canales', nuevoCanal, 1);
    }
    if (nuevoDisp !== prev.disp_actual) {
      await redis.hIncrBy('kpi:dispositivos', prev.disp_actual, -1);
      await redis.hIncrBy('kpi:dispositivos', nuevoDisp, 1);
    }

    const kpisActualizados = await obtenerKPIs();
    io.emit('actualizacion_kpis', { accion: 'PUT', id_interaccion: id, kpis: kpisActualizados });

    res.json({ mensaje: 'Interacción actualizada y reflejada en tiempo real', id_interaccion: id, kpis: kpisActualizados });
  } catch (err) {
    await conn.rollback();
    console.error('Detalle error PUT:', err);
    res.status(500).json({ error: 'Error al actualizar la interacción', detalle: err.sqlMessage || err.message });
  } finally {
    conn.release();
  }
});

// 6. DELETE: Eliminar interacción (desde Postman) -> TiDB + Redis + WebSocket
app.delete('/api/interacciones/:id', verificarToken, async (req, res) => {
  const conn = await pool.getConnection();
  try {
    const { id } = req.params;

    const [prevRows] = await conn.execute(`
      SELECT i.sentimiento, c.nombre AS canal, t.nombre AS tema, d.nombre AS dispositivo
      FROM interacciones i
      JOIN canales c ON i.id_canal = c.id_canal
      JOIN temas t ON i.id_tema = t.id_tema
      JOIN dispositivos d ON i.id_dispositivo = d.id_dispositivo
      WHERE i.id_interaccion = ?
    `, [id]);

    if (prevRows.length === 0) {
      return res.status(404).json({ error: `Interacción ${id} no encontrada` });
    }

    const prev = prevRows[0];

    await conn.beginTransaction();
    await conn.execute('DELETE FROM interacciones WHERE id_interaccion = ?', [id]);
    await conn.commit();

    await redis.hIncrBy('kpi:resumen', 'total_interacciones', -1);
    await redis.hIncrByFloat('kpi:resumen', 'suma_sentimiento', -Number(prev.sentimiento));
    await redis.hIncrBy('kpi:temas', prev.tema, -1);
    await redis.hIncrBy('kpi:canales', prev.canal, -1);
    await redis.hIncrBy('kpi:dispositivos', prev.dispositivo, -1);

    const kpisActualizados = await obtenerKPIs();
    io.emit('actualizacion_kpis', { accion: 'DELETE', id_interaccion: id, kpis: kpisActualizados });

    res.json({ mensaje: 'Interacción eliminada y reflejada en tiempo real', id_interaccion: id, kpis: kpisActualizados });
  } catch (err) {
    await conn.rollback();
    console.error('Detalle error DELETE:', err);
    res.status(500).json({ error: 'Error al eliminar la interacción', detalle: err.sqlMessage || err.message });
  } finally {
    conn.release();
  }
});

io.on('connection', (socket) => {
  console.log('Cliente conectado por WebSocket:', socket.id);
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Servidor Community 360 activo en http://localhost:${PORT}`);
});
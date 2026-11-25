import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse/sync';
import mysql from 'mysql2/promise';
import { createClient } from 'redis';
import bcrypt from 'bcryptjs';
import 'dotenv/config';

const parseFechaDMY = (str) => {
  const [d, m, y] = str.trim().split('/');
  return `${y}-${m.padStart(2, '0')}-${d.padStart(2, '0')}`;
};

const cleanNull = (val) => {
  if (val === undefined || val === null) return null;
  const trimmed = String(val).trim();
  return trimmed === '' || trimmed.toLowerCase() === 'null' ? null : trimmed;
};

async function runETL() {
  console.log('Iniciando proceso ETL de 8 etapas para Community 360...\n');

  const rawUsers = parse(fs.readFileSync('comunidad_virtual_usuarios.csv'), {
    columns: true,
    skip_empty_lines: true,
    bom: true
  });
  const rawInter = parse(fs.readFileSync('comunidad_virtual_1000_interacciones.csv'), {
    columns: true,
    skip_empty_lines: true,
    bom: true
  });

  // =========================================================================
  // ETAPA 1: Eliminación de registros duplicados (id_usuario, alias, id_interaccion)
  // =========================================================================
  const seenUserIds = new Set();
  const seenAliases = new Set();
  const uniqueUsers = [];
  let dupUsers = 0;

  for (const u of rawUsers) {
    const id = u.id_usuario.trim();
    const aliasKey = u.alias.trim().toLowerCase();
    if (seenUserIds.has(id) || seenAliases.has(aliasKey)) {
      dupUsers++;
      continue;
    }
    seenUserIds.add(id);
    seenAliases.add(aliasKey);
    uniqueUsers.push(u);
  }

  const seenInterIds = new Set();
  const uniqueInter = [];
  let dupInter = 0;

  for (const i of rawInter) {
    const id = i.id_interaccion.trim();
    if (seenInterIds.has(id)) {
      dupInter++;
      continue;
    }
    seenInterIds.add(id);
    uniqueInter.push(i);
  }
  console.log(`[Etapa 1] Duplicados eliminados -> Usuarios: ${dupUsers}, Interacciones: ${dupInter}`);

  // =========================================================================
  // ETAPAS 2, 3, 4 y 6: Nulos, estandarización, tipos y privacidad de usuarios
  // =========================================================================
  const usuariosLimpios = [];
  const ubicacionesLimpias = [];
  const metricasLimpias = [];
  const consentMap = new Map();
  let nulosNormalizadosUsers = 0;
  let anonimizados = 0;

  for (const u of uniqueUsers) {
    const id = u.id_usuario.trim();
    const consentimiento = u.consentimiento_datos.trim().toLowerCase() === 'si';
    const esArtista = Number(u.es_artista) === 1;
    consentMap.set(id, consentimiento);

    // Etapa 4: Validación estricta de tipos numéricos y fechas
    const mensajes = Number.parseInt(u.mensajes_30d, 10) || 0;
    const reacciones = Number.parseInt(u.reacciones_30d, 10) || 0;
    const eventos = Number.parseInt(u.eventos_asistidos, 10) || 0;
    const comisiones = Number.parseInt(u.comisiones_compradas, 10) || 0;
    const sentimientoProm = Number.parseFloat(u.sentimiento_promedio) || 0.0;
    const fechaIngreso = parseFechaDMY(u.fecha_ingreso);
    const ultimaActividad = parseFechaDMY(u.ultima_actividad);

    // Etapa 2 y 6: Normalización de nulos y supresión geográfica sin consentimiento
    let pais = cleanNull(u.pais);
    let estado = cleanNull(u.estado);
    let ciudad = cleanNull(u.ciudad);
    let latitud = cleanNull(u.latitud) !== null ? Number.parseFloat(u.latitud) : null;
    let longitud = cleanNull(u.longitud) !== null ? Number.parseFloat(u.longitud) : null;
    let zonaHoraria = cleanNull(u.zona_horaria);

    if (latitud === null || longitud === null) nulosNormalizadosUsers++;

    if (!consentimiento) {
      anonimizados++;
      pais = null;
      estado = null;
      ciudad = null;
      latitud = null;
      longitud = null;
      zonaHoraria = null;
    }

    usuariosLimpios.push({
      id_usuario: id,
      alias: u.alias.trim(),
      fecha_ingreso: fechaIngreso,
      rango_edad: u.rango_edad.trim(),
      especie_fursona: u.especie_fursona.trim(),
      interes_principal: u.interes_principal.trim(),
      plataforma_principal: u.plataforma_principal.trim(),
      es_artista: esArtista,
      estatus: u.estatus.trim().toLowerCase(),
      consentimiento_datos: consentimiento
    });

    ubicacionesLimpias.push({
      id_usuario: id,
      pais,
      estado,
      ciudad,
      latitud,
      longitud,
      zona_horaria: zonaHoraria
    });

    metricasLimpias.push({
      id_usuario: id,
      mensajes_30d: mensajes,
      reacciones_30d: reacciones,
      eventos_asistidos: eventos,
      comisiones_compradas: comisiones,
      sentimiento_promedio: sentimientoProm,
      ultima_actividad: ultimaActividad
    });
  }

  // =========================================================================
  // ETAPA 5: Verificación de integridad referencial y depuración de interacciones
  // =========================================================================
  const interaccionesLimpias = [];
  const cuarentena = [];
  let nulosTexto = 0;

  for (const i of uniqueInter) {
    const idInter = i.id_interaccion.trim();
    const idUser = i.id_usuario.trim();

    if (!consentMap.has(idUser)) {
      cuarentena.push({
        id_interaccion: idInter,
        id_usuario_reportado: idUser,
        motivo_rechazo: 'Integridad referencial: id_usuario inexistente en padrón',
        datos_crudos: i
      });
      continue;
    }

    const textoLimpio = cleanNull(i.texto);
    if (textoLimpio === null) nulosTexto++;

    const etiquetasArray = cleanNull(i.etiquetas)
      ? [...new Set(i.etiquetas.split('|').map((t) => t.trim().toLowerCase()).filter(Boolean))]
      : [];

    interaccionesLimpias.push({
      id_interaccion: idInter,
      id_usuario: idUser,
      timestamp: i.timestamp.trim().replace('T', ' ').replace('Z', ''),
      canal: i.canal.trim(),
      tipo: i.tipo.trim().toLowerCase(),
      texto: textoLimpio,
      tema: i.tema.trim(),
      sentimiento: Number.parseFloat(i.sentimiento) || 0.0,
      idioma: i.idioma.trim().toLowerCase(),
      dispositivo: i.dispositivo.trim(),
      longitud_texto: Number.parseInt(i.longitud_texto, 10) || 0,
      reacciones_recibidas: Number.parseInt(i.reacciones_recibidas, 10) || 0,
      etiquetas: etiquetasArray
    });
  }

  console.log(`[Etapa 2-4] Nulos normalizados -> Coordenadas vacías usuarios: ${nulosNormalizadosUsers}, Textos vacíos reacciones: ${nulosTexto}`);
  console.log(`[Etapa 5] Integridad referencial -> Aceptadas: ${interaccionesLimpias.length}, Cuarentena (huérfanas): ${cuarentena.length}`);
  console.log(`[Etapa 6] Privacidad aplicada -> Usuarios anonimizados (consentimiento = no): ${anonimizados}`);

  // =========================================================================
  // ETAPA 7: Generación de archivos depurados locales para auditoría
  // =========================================================================
  const outDir = path.join(process.cwd(), 'datos_depurados');
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir);

  fs.writeFileSync(path.join(outDir, 'usuarios_limpios.json'), JSON.stringify(usuariosLimpios, null, 2));
  fs.writeFileSync(path.join(outDir, 'usuario_ubicaciones_limpias.json'), JSON.stringify(ubicacionesLimpias, null, 2));
  fs.writeFileSync(path.join(outDir, 'usuario_metricas_limpias.json'), JSON.stringify(metricasLimpias, null, 2));
  fs.writeFileSync(path.join(outDir, 'interacciones_limpias.json'), JSON.stringify(interaccionesLimpias, null, 2));
  fs.writeFileSync(path.join(outDir, 'interacciones_cuarentena.json'), JSON.stringify(cuarentena, null, 2));

  console.log(`[Etapa 7] Archivos depurados exportados en: ${outDir}`);

  // =========================================================================
  // ETAPA 8: Carga híbrida en TiDB Cloud (SQL) y Redis Cloud (NoSQL)
  // =========================================================================
  console.log('[Etapa 8] Conectando a TiDB Cloud y Redis Cloud...');
  const sql = await mysql.createConnection({
    host: process.env.TIDB_HOST,
    port: Number(process.env.TIDB_PORT),
    user: process.env.TIDB_USER,
    password: process.env.TIDB_PASSWORD,
    database: process.env.TIDB_DATABASE,
    ssl: { minVersion: 'TLSv1.2', rejectUnauthorized: true }
  });

  const redis = createClient({ url: process.env.REDIS_URL });
  await redis.connect();

  await sql.execute('DELETE FROM cuarentena_interacciones');

  for (let idx = 0; idx < usuariosLimpios.length; idx++) {
    const u = usuariosLimpios[idx];
    const ub = ubicacionesLimpias[idx];
    const me = metricasLimpias[idx];

    await sql.execute('REPLACE INTO usuarios VALUES (?,?,?,?,?,?,?,?,?,?)', [
      u.id_usuario, u.alias, u.fecha_ingreso, u.rango_edad, u.especie_fursona,
      u.interes_principal, u.plataforma_principal, u.es_artista, u.estatus, u.consentimiento_datos
    ]);
    await sql.execute('REPLACE INTO usuario_ubicaciones VALUES (?,?,?,?,?,?,?)', [
      ub.id_usuario, ub.pais, ub.estado, ub.ciudad, ub.latitud, ub.longitud, ub.zona_horaria
    ]);
    await sql.execute('REPLACE INTO usuario_metricas VALUES (?,?,?,?,?,?,?)', [
      me.id_usuario, me.mensajes_30d, me.reacciones_30d, me.eventos_asistidos,
      me.comisiones_compradas, me.sentimiento_promedio, me.ultima_actividad
    ]);
  }

  const getCatalogId = async (tabla, colId, nombre) => {
    await sql.execute(`INSERT IGNORE INTO ${tabla} (nombre) VALUES (?)`, [nombre]);
    const [[row]] = await sql.execute(`SELECT ${colId} AS id FROM ${tabla} WHERE nombre = ?`, [nombre]);
    return row.id;
  };

  const temasCount = {};
  const canalesCount = {};
  const dispositivosCount = {};
  let sumaSentimiento = 0;

  for (const inter of interaccionesLimpias) {
    const idCanal = await getCatalogId('canales', 'id_canal', inter.canal);
    const idTema = await getCatalogId('temas', 'id_tema', inter.tema);
    const idDispositivo = await getCatalogId('dispositivos', 'id_dispositivo', inter.dispositivo);

    await sql.execute('REPLACE INTO interacciones VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', [
      inter.id_interaccion, inter.id_usuario, idCanal, idTema, idDispositivo,
      inter.timestamp, inter.tipo, inter.texto, inter.sentimiento,
      inter.idioma, inter.longitud_texto, inter.reacciones_recibidas
    ]);

    for (const tag of inter.etiquetas) {
      const idEtiqueta = await getCatalogId('etiquetas', 'id_etiqueta', tag);
      await sql.execute('INSERT IGNORE INTO interaccion_etiquetas VALUES (?,?)', [inter.id_interaccion, idEtiqueta]);
    }

    temasCount[inter.tema] = (temasCount[inter.tema] || 0) + 1;
    canalesCount[inter.canal] = (canalesCount[inter.canal] || 0) + 1;
    dispositivosCount[inter.dispositivo] = (dispositivosCount[inter.dispositivo] || 0) + 1;
    sumaSentimiento += inter.sentimiento;
  }

  for (const q of cuarentena) {
    await sql.execute(
      'INSERT INTO cuarentena_interacciones (id_interaccion, id_usuario_reportado, motivo_rechazo, datos_crudos) VALUES (?,?,?,?)',
      [q.id_interaccion, q.id_usuario_reportado, q.motivo_rechazo, JSON.stringify(q.datos_crudos)]
    );
  }

  const hash = await bcrypt.hash('Admin360*', 10);
  await sql.execute('INSERT IGNORE INTO usuarios_sistema (username, password_hash) VALUES (?,?)', ['admin', hash]);

  await redis.del('kpi:temas', 'kpi:canales', 'kpi:dispositivos', 'kpi:resumen');
  await redis.hSet('kpi:temas', temasCount);
  await redis.hSet('kpi:canales', canalesCount);
  await redis.hSet('kpi:dispositivos', dispositivosCount);
  await redis.hSet('kpi:resumen', {
    total_usuarios: String(usuariosLimpios.length),
    total_interacciones: String(interaccionesLimpias.length),
    total_cuarentena: String(cuarentena.length),
    suma_sentimiento: sumaSentimiento.toFixed(4)
  });

  console.log('[Etapa 8] Carga completada exitosamente en TiDB Cloud y Redis Cloud.');
  await redis.disconnect();
  await sql.end();
}

runETL().catch(console.error);
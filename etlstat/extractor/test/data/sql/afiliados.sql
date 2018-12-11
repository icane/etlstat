SELECT
  sce.alumnos.curso,
  sce.fechas.codigo,
  sce.fechas.especialidad,
  sce.fechas.centro,
  sce.fechas.localidad,
  sce.fechas.horas,
  sce.alumnos.id_alumno,
  sce.fechas.f_inicio,
  sce.fechas.f_final,
  sce.afiliados.fecha_alta,
  sce.afiliados.fecha_ref,
  sce.afiliados.gc,
  sce.afiliados.desc_gc,
  sce.afiliados.cnae2,
  sce.afiliados.desc_cnae2,
  sce.afiliados.cnae3,
  sce.afiliados.desc_cnae3,
  sce.afiliados.cnae4,
  sce.afiliados.desc_cnae4,
  sce.afiliados.regimen,


  CASE
  WHEN afiliados.fecha_alta < fechas.f_inicio
    THEN 1
  ELSE 0
  END AS afiliado_anterior,

  CASE
  WHEN afiliados.fecha_alta BETWEEN fechas.f_inicio AND fechas.f_final
    THEN 1
  ELSE 0
  END AS afiliado_durante_curso,
  CASE
  WHEN sce.afiliados.fecha_alta BETWEEN fechas.f_final AND fechas.6_meses_final
    THEN 1
  ELSE 0
  END AS afiliado_antes_6_meses,
  CASE
  WHEN sce.afiliados.fecha_alta BETWEEN fechas.f_final AND fechas.12_meses_final
    THEN 1
  ELSE 0
  END AS afiliado_antes_12_meses,
  CASE
  WHEN afiliados.fecha_alta > fechas.12_meses_final
    THEN 1
  ELSE 0
  END AS afiliado_despues_12_meses,
  CASE
  WHEN sce.afiliados.nif IS NULL
    THEN 0
  ELSE 1
  END AS afiliado,
  CASE
  WHEN sce.afiliados.regimen = 'R.E. Trabajadores Autónomos'
    THEN 1
  ELSE 0
  END AS reta

FROM
  sce.alumnos
  NATURAL LEFT JOIN sce.contratos
  INNER JOIN sce.fechas ON sce.alumnos.curso = sce.fechas.curso
  LEFT JOIN sce.afiliados ON sce.alumnos.nif = sce.afiliados.nif
WHERE sce.contratos.nif IS NULL
  AND sce.afiliados.nif IS NOT NULL
ORDER BY curso, id_alumno ASC
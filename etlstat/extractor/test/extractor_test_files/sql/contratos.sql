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
            sce.contratos.fecha_inicio,
            sce.contratos.fecha_registro,
            sce.contratos.cnae2,
            sce.contratos.desc_cnae2,
            sce.contratos.cno,
            sce.contratos.desc_cno,

            CASE
                WHEN contratos.fecha_inicio < fechas.f_inicio THEN 1
                ELSE 0
            END as contrato_anterior,

            CASE
                WHEN contratos.fecha_inicio BETWEEN fechas.f_inicio AND fechas.f_final THEN 1
                ELSE 0
            END as contrato_durante_curso,
            CASE
                WHEN sce.contratos.fecha_inicio BETWEEN fechas.f_final AND fechas.6_meses_final  THEN 1
                ELSE 0
            END AS contrato_antes_6_meses,
            CASE
                WHEN sce.contratos.fecha_inicio BETWEEN fechas.f_final AND fechas.12_meses_final  THEN 1
                ELSE 0
            END AS contrato_antes_12_meses,
            CASE
                WHEN contratos.fecha_inicio > fechas.12_meses_final THEN 1
                ELSE 0
            END as contrato_despues_12_meses,
            CASE
                WHEN sce.contratos.nif IS NULL THEN 0
                ELSE 1
            END AS contrato,
            CASE
                WHEN afiliados.fecha_alta < fechas.f_inicio THEN 1
                ELSE 0
            END as afiliado_anterior,
            CASE
                WHEN afiliados.fecha_alta BETWEEN fechas.f_inicio AND fechas.f_final THEN 1
                ELSE 0
            END as afiliado_durante_curso,
            CASE
                WHEN sce.afiliados.fecha_alta BETWEEN fechas.f_final AND fechas.6_meses_final  THEN 1
                ELSE 0
            END AS afiliado_antes_6_meses,
            CASE
                WHEN sce.afiliados.fecha_alta BETWEEN fechas.f_final AND fechas.12_meses_final  THEN 1
                ELSE 0
            END AS afiliado_antes_12_meses,
            CASE
                WHEN afiliados.fecha_alta > fechas.12_meses_final THEN 1
                ELSE 0
            END as afiliado_despues_12_meses,
            CASE
                WHEN sce.afiliados.nif IS NULL THEN 0
                ELSE 1
            END AS afiliado,
            CASE
                WHEN sce.afiliados.regimen = 'R.E. Trabajadores Autónomos' THEN 1
                ELSE 0
            END AS reta

    FROM
        sce.alumnos
    INNER JOIN sce.contratos ON sce.alumnos.nif = sce.contratos.nif
    INNER JOIN sce.fechas ON sce.alumnos.curso = sce.fechas.curso
    INNER JOIN sce.ocupaciones ON sce.fechas.codigo = sce.ocupaciones.codigo
    LEFT JOIN sce.afiliados ON sce.alumnos.nif = sce.afiliados.nif
    WHERE sce.ocupaciones.oc_rel_1 = 'NO FIGURA OCUPACION'
    ORDER BY curso, id_alumno
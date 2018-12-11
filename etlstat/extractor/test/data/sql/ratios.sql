SELECT
    curso,
    codigo,
    especialidad,
    centro,
    localidad,
    horas,
    SUM(contrato_) / COUNT(*) AS ratio_contratos,
    SUM(valido_cno_) / COUNT(*) AS ratio_valido_cno,
    SUM(sin_ocupacion_) / COUNT(*) AS ratio_sin_ocupacion,
    SUM(contrato_anterior_) / COUNT(*) AS ratio_contrato_anterior,
    SUM(contrato_durante_curso_) / COUNT(*) AS ratio_contrato_durante_curso,
    SUM(contrato_antes_6_meses_) / COUNT(*) AS ratio_contrato_antes_6_meses,
    SUM(contrato_antes_12_meses_) / COUNT(*) AS ratio_contrato_antes_12_meses,
    SUM(contrato_despues_12_meses_) / COUNT(*) AS ratio_contrato_despues_12_meses,
    SUM(contrato_anterior_cno_valido_) / COUNT(*) AS ratio_contrato_anterior_cno_valido,
    SUM(contrato_durante_curso_cno_valido_) / COUNT(*) AS ratio_contrato_durante_curso_cno_valido,
    SUM(contrato_antes_6_meses_cno_valido_) / COUNT(*) AS ratio_contrato_antes_6_meses_cno_valido,
    SUM(contrato_antes_12_meses_cno_valido_) / COUNT(*) AS ratio_contrato_antes_12_meses_cno_valido,
    SUM(contrato_despues_12_meses_cno_valido_) / COUNT(*) AS ratio_contrato_despues_12_meses_cno_valido,
    SUM(afiliado_anterior_) / COUNT(*) AS ratio_afiliado_anterior,
    SUM(afiliado_durante_curso_) / COUNT(*) AS ratio_afiliado_durante_curso,
    SUM(afiliado_antes_6_meses_) / COUNT(*) AS ratio_afiliado_antes_6_meses,
    SUM(afiliado_antes_12_meses_) / COUNT(*) AS ratio_afiliado_antes_12_meses,
    SUM(afiliado_despues_12_meses_) / COUNT(*) AS ratio_afiliado_despues_12_meses,
    SUM(afiliado_) / COUNT(*) AS ratio_afiliado,
    SUM(reta_) / COUNT(*) AS ratio_reta,
    SUM(afiliado_anterior_sin_contrato_) / COUNT(*) AS ratio_afiliado_anterior_sin_contrato,
    SUM(afiliado_durante_curso_sin_contrato_) / COUNT(*) AS ratio_afiliado_durante_curso_sin_contrato,
    SUM(afiliado_antes_6_meses_sin_contrato_) / COUNT(*) AS ratio_afiliado_antes_6_meses_sin_contrato,
    SUM(afiliado_antes_12_meses_sin_contrato_) / COUNT(*) AS ratio_afiliado_antes_12_meses_sin_contrato,
    SUM(afiliado_despues_12_meses_sin_contrato_) / COUNT(*) AS ratio_afiliado_despues_12_meses_sin_contrato,
    SUM(afiliado_sin_contrato_) / COUNT(*) AS ratio_afiliado_sin_contrato,
    SUM(reta_sin_contrato_) / COUNT(*) AS ratio_reta_sin_contrato
FROM
    (SELECT
            curso,
            codigo,
            especialidad,
            centro,
            localidad,
            horas,
            nif,
            nombre,
            CASE
                WHEN SUM(contrato) > 0 THEN 1
                ELSE 0
            END AS contrato_,
            CASE
                WHEN SUM(valido_cno) > 0 THEN 1
                ELSE 0
            END AS valido_cno_,
            CASE
                WHEN SUM(sin_ocupacion) > 0 THEN 1
                ELSE 0
            END AS sin_ocupacion_,
            CASE
                WHEN SUM(contrato_anterior) > 0 THEN 1
                ELSE 0
            END AS contrato_anterior_,
            CASE
                WHEN SUM(contrato_durante_curso) > 0 THEN 1
                ELSE 0
            END AS contrato_durante_curso_,
            CASE
                WHEN SUM(contrato_antes_6_meses) > 0 THEN 1
                ELSE 0
            END AS contrato_antes_6_meses_,
            CASE
                WHEN SUM(contrato_antes_12_meses) > 0 THEN 1
                ELSE 0
            END AS contrato_antes_12_meses_,
            CASE
                WHEN SUM(contrato_despues_12_meses) > 0 THEN 1
                ELSE 0
            END AS contrato_despues_12_meses_,
             CASE
                WHEN SUM(contrato_anterior_cno_valido) > 0 THEN 1
                ELSE 0
            END AS contrato_anterior_cno_valido_,
            CASE
                WHEN SUM(contrato_durante_curso_cno_valido) > 0 THEN 1
                ELSE 0
            END AS contrato_durante_curso_cno_valido_,
            CASE
                WHEN SUM(contrato_antes_6_meses_cno_valido) > 0 THEN 1
                ELSE 0
            END AS contrato_antes_6_meses_cno_valido_,
            CASE
                WHEN SUM(contrato_antes_12_meses_cno_valido) > 0 THEN 1
                ELSE 0
            END AS contrato_antes_12_meses_cno_valido_,
            CASE
                WHEN SUM(contrato_despues_12_meses_cno_valido) > 0 THEN 1
                ELSE 0
            END AS contrato_despues_12_meses_cno_valido_,
            CASE
                WHEN SUM(afiliado_anterior) > 0 THEN 1
                ELSE 0
            END AS afiliado_anterior_,
            CASE
                WHEN SUM(afiliado_durante_curso) > 0 THEN 1
                ELSE 0
            END AS afiliado_durante_curso_,
            CASE
                WHEN SUM(afiliado_antes_6_meses) > 0 THEN 1
                ELSE 0
            END AS afiliado_antes_6_meses_,
            CASE
                WHEN SUM(afiliado_antes_12_meses) > 0 THEN 1
                ELSE 0
            END AS afiliado_antes_12_meses_,
            CASE
                WHEN SUM(afiliado_despues_12_meses) > 0 THEN 1
                ELSE 0
            END AS afiliado_despues_12_meses_,
            CASE
                WHEN SUM(afiliado) > 0 THEN 1
                ELSE 0
            END AS afiliado_,
            CASE
                WHEN SUM(reta) > 0 THEN 1
                ELSE 0
            END AS reta_,
            CASE
                WHEN SUM(afiliado_anterior_sin_contrato) > 0 THEN 1
                ELSE 0
            END AS afiliado_anterior_sin_contrato_,
            CASE
                WHEN SUM(afiliado_durante_curso_sin_contrato) > 0 THEN 1
                ELSE 0
            END AS afiliado_durante_curso_sin_contrato_,
            CASE
                WHEN SUM(afiliado_antes_6_meses_sin_contrato) > 0 THEN 1
                ELSE 0
            END AS afiliado_antes_6_meses_sin_contrato_,
            CASE
                WHEN SUM(afiliado_antes_12_meses_sin_contrato) > 0 THEN 1
                ELSE 0
            END AS afiliado_antes_12_meses_sin_contrato_,
            CASE
                WHEN SUM(afiliado_despues_12_meses_sin_contrato) > 0 THEN 1
                ELSE 0
            END AS afiliado_despues_12_meses_sin_contrato_,
            CASE
                WHEN SUM(afiliado_sin_contrato) > 0 THEN 1
                ELSE 0
            END AS afiliado_sin_contrato_,

            CASE
                WHEN SUM(reta_sin_contrato) > 0 THEN 1
                ELSE 0
            END AS reta_sin_contrato_
    FROM
        (SELECT
            sce.alumnos.curso,
            sce.fechas.codigo,
            sce.fechas.especialidad,
            sce.fechas.centro,
            sce.fechas.localidad,
            sce.fechas.horas,
            sce.alumnos.nif,
            sce.alumnos.nombre,
            CASE
                WHEN sce.contratos.cno IN (sce.ocupaciones.oc_rel_1 , sce.ocupaciones.oc_rel_2, sce.ocupaciones.oc_rel_3, sce.ocupaciones.oc_rel_4, sce.ocupaciones.oc_rel_5, sce.ocupaciones.oc_rel_6, sce.ocupaciones.oc_rel_7) THEN 1
                ELSE 0
            END AS valido_cno,
            CASE
                WHEN sce.ocupaciones.oc_rel_1 = 'NO FIGURA OCUPACION' THEN 1
                ELSE 0
            END AS sin_ocupacion,
            CASE
                WHEN sce.contratos.fecha_inicio BETWEEN fechas.f_final AND fechas.6_meses_final  THEN 1
                ELSE 0
            END AS contrato_antes_6_meses,
            CASE
                WHEN sce.contratos.fecha_inicio BETWEEN fechas.f_final AND fechas.12_meses_final  THEN 1
                ELSE 0
            END AS contrato_antes_12_meses,
             CASE
                WHEN contratos.fecha_inicio < fechas.f_inicio AND sce.contratos.cno IN (sce.ocupaciones.oc_rel_1 , sce.ocupaciones.oc_rel_2, sce.ocupaciones.oc_rel_3, sce.ocupaciones.oc_rel_4, sce.ocupaciones.oc_rel_5, sce.ocupaciones.oc_rel_6, sce.ocupaciones.oc_rel_7) THEN 1
                ELSE 0
            END as contrato_anterior_cno_valido,

            CASE
                WHEN contratos.fecha_inicio BETWEEN fechas.f_inicio AND fechas.f_final AND sce.contratos.cno IN (sce.ocupaciones.oc_rel_1 , sce.ocupaciones.oc_rel_2, sce.ocupaciones.oc_rel_3, sce.ocupaciones.oc_rel_4, sce.ocupaciones.oc_rel_5, sce.ocupaciones.oc_rel_6, sce.ocupaciones.oc_rel_7) THEN 1
                ELSE 0
            END as contrato_durante_curso_cno_valido,

            CASE
                WHEN contratos.fecha_inicio > fechas.12_meses_final AND sce.contratos.cno IN (sce.ocupaciones.oc_rel_1 , sce.ocupaciones.oc_rel_2, sce.ocupaciones.oc_rel_3, sce.ocupaciones.oc_rel_4, sce.ocupaciones.oc_rel_5, sce.ocupaciones.oc_rel_6, sce.ocupaciones.oc_rel_7) THEN 1
                ELSE 0
            END as contrato_despues_12_meses_cno_valido,
            CASE
                WHEN sce.contratos.fecha_inicio BETWEEN fechas.f_final AND fechas.6_meses_final AND sce.contratos.cno IN (sce.ocupaciones.oc_rel_1 , sce.ocupaciones.oc_rel_2, sce.ocupaciones.oc_rel_3, sce.ocupaciones.oc_rel_4, sce.ocupaciones.oc_rel_5, sce.ocupaciones.oc_rel_6, sce.ocupaciones.oc_rel_7) THEN 1
                ELSE 0
            END AS contrato_antes_6_meses_cno_valido,
            CASE
                WHEN sce.contratos.fecha_inicio BETWEEN fechas.f_final AND fechas.12_meses_final AND sce.contratos.cno IN (sce.ocupaciones.oc_rel_1 , sce.ocupaciones.oc_rel_2, sce.ocupaciones.oc_rel_3, sce.ocupaciones.oc_rel_4, sce.ocupaciones.oc_rel_5, sce.ocupaciones.oc_rel_6, sce.ocupaciones.oc_rel_7) THEN 1
                ELSE 0
            END AS contrato_antes_12_meses_cno_valido,
            CASE
                WHEN sce.contratos.nif IS NULL THEN 0
                ELSE 1
            END AS contrato,
            CASE
                WHEN sce.afiliados.fecha_alta BETWEEN fechas.f_final AND fechas.6_meses_final  THEN 1
                ELSE 0
            END AS afiliado_antes_6_meses,
            CASE
                WHEN sce.afiliados.fecha_alta BETWEEN fechas.f_final AND fechas.12_meses_final  THEN 1
                ELSE 0
            END AS afiliado_antes_12_meses,
            CASE
                WHEN sce.afiliados.nif IS NULL THEN 0
                ELSE 1
            END AS afiliado,
            CASE
                WHEN sce.afiliados.regimen = 'R.E. Trabajadores Autónomos' THEN 1
                ELSE 0
            END AS reta,
            CASE
                WHEN
                    sce.afiliados.fecha_alta BETWEEN fechas.f_final AND fechas.6_meses_final
                        AND sce.contratos.nif IS NULL
                THEN
                    1
                ELSE 0
            END AS afiliado_antes_6_meses_sin_contrato,
            CASE
                WHEN
                    sce.afiliados.fecha_alta BETWEEN fechas.f_final AND fechas.12_meses_final
                        AND sce.contratos.nif IS NULL
                THEN
                    1
                ELSE 0
            END AS afiliado_antes_12_meses_sin_contrato,
            CASE
                WHEN
                    sce.afiliados.fecha_alta > fechas.12_meses_final
                        AND sce.contratos.nif IS NULL
                THEN
                    1
                ELSE 0
            END AS afiliado_despues_12_meses_sin_contrato,
             CASE
                WHEN
                    sce.afiliados.fecha_alta BETWEEN fechas.f_inicio AND fechas.f_final
                        AND sce.contratos.nif IS NULL
                THEN
                    1
                ELSE 0
            END AS afiliado_durante_curso_sin_contrato,
               CASE
                WHEN
                    sce.afiliados.fecha_alta < fechas.f_inicio
                        AND sce.contratos.nif IS NULL
                THEN
                    1
                ELSE 0
            END AS afiliado_anterior_sin_contrato,

            CASE
                WHEN
                    sce.afiliados.nif IS NOT NULL
                        AND sce.contratos.nif IS NULL
                THEN
                    1
                ELSE 0
            END AS afiliado_sin_contrato,
            CASE
                WHEN
                    sce.afiliados.regimen = 'R.E. Trabajadores Autónomos'
                        AND sce.contratos.nif IS NULL
                THEN
                    1
                ELSE 0
            END AS reta_sin_contrato,
            CASE
                WHEN afiliados.fecha_alta < fechas.f_inicio THEN 1
                ELSE 0
            END as afiliado_anterior,
            CASE
                WHEN afiliados.fecha_alta BETWEEN fechas.f_inicio AND fechas.f_final THEN 1
                ELSE 0
            END as afiliado_durante_curso,

            CASE
                WHEN afiliados.fecha_alta > fechas.12_meses_final THEN 1
                ELSE 0
            END as afiliado_despues_12_meses,

            CASE
                WHEN contratos.fecha_inicio < fechas.f_inicio THEN 1
                ELSE 0
            END as contrato_anterior,

            CASE
                WHEN contratos.fecha_inicio BETWEEN fechas.f_inicio AND fechas.f_final THEN 1
                ELSE 0
            END as contrato_durante_curso,

            CASE
                WHEN contratos.fecha_inicio > fechas.12_meses_final THEN 1
                ELSE 0
            END as contrato_despues_12_meses

    FROM
        sce.alumnos
    LEFT JOIN sce.contratos ON sce.alumnos.nif = sce.contratos.nif
    INNER JOIN sce.fechas ON sce.alumnos.curso = sce.fechas.curso
    INNER JOIN sce.ocupaciones ON sce.fechas.codigo = sce.ocupaciones.codigo
    LEFT JOIN sce.afiliados ON sce.alumnos.nif = sce.afiliados.nif
    ) AS consulta
    GROUP BY curso , nif
    ORDER BY curso , nif) AS agregados
GROUP BY curso
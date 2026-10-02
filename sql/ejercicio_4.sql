-- CC3084 - Lab 8: DuckDB
-- Ejercicio 4 - Analisis exploratorio utilizando DuckDB
--
-- Consultas de notebooks/ejercicio_4.ipynb. Todas leen directamente los archivos
-- Parquet de data/raw/ a traves de las vistas viajes y viajes_limpios. Las rutas
-- son relativas a la raiz del proyecto (/workspace dentro del contenedor lab).
-- Las preguntas P1 a P12 corresponden al inciso 4.1 del notebook.

-- Preparacion: Vista viajes: unifica amarillos y verdes y marca el bloque sin metadatos
CREATE OR REPLACE VIEW viajes AS
SELECT regexp_extract(filename, '(yellow|green)_tripdata', 1)         AS tipo,
       regexp_extract(filename, '_(\d{4}-\d{2})\.parquet', 1) AS mes_archivo,
       COALESCE(tpep_pickup_datetime, lpep_pickup_datetime)            AS pickup,
       COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime)          AS dropoff,
       passenger_count IS NULL AND RatecodeID IS NULL                  AS sin_metadatos,
       *
FROM read_parquet('data/raw/*/*/*.parquet', filename = true, union_by_name = true);


-- Preparacion: Comparacion del bloque sin metadatos contra el resto de viajes
SELECT tipo,
       sin_metadatos,
       COUNT(*)                                             AS viajes,
       ROUND(MEDIAN(trip_distance), 2)                      AS distancia_mediana,
       ROUND(MEDIAN(epoch(dropoff - pickup) / 60), 1)       AS duracion_mediana_min,
       ROUND(MEDIAN(total_amount), 2)                       AS total_mediano,
       ROUND(100 * AVG((tip_amount > 0)::INT), 1)           AS pct_con_propina
FROM viajes
GROUP BY tipo, sin_metadatos
ORDER BY tipo DESC, sin_metadatos;


-- Preparacion: Proporcion del bloque por proveedor
SELECT tipo,
       VendorID,
       COUNT(*)                                   AS viajes,
       ROUND(100 * AVG(sin_metadatos::INT), 1)    AS pct_sin_metadatos
FROM viajes
GROUP BY tipo, VendorID
ORDER BY tipo DESC, VendorID;


-- Preparacion: Proporcion del bloque por hora del dia (amarillos)
SELECT hour(pickup)                               AS hora,
       ROUND(100 * AVG(sin_metadatos::INT), 1)    AS pct_sin_metadatos
FROM viajes
WHERE tipo = 'yellow'
GROUP BY hora
ORDER BY hora;


-- Preparacion: Vista viajes_limpios: excluye registros invalidos
CREATE OR REPLACE VIEW viajes_limpios AS
SELECT *,
       epoch(dropoff - pickup) / 60 AS duracion_min
FROM viajes
WHERE strftime(pickup, '%Y-%m') = mes_archivo
  AND dropoff > pickup AND dropoff - pickup <= INTERVAL 24 HOUR
  AND trip_distance > 0 AND trip_distance <= 100
  AND fare_amount >= 0 AND total_amount > 0;


-- Preparacion: Registros afectados por cada regla de limpieza
SELECT tipo,
       COUNT(*)                                                                         AS total,
       COUNT(*) FILTER (WHERE strftime(pickup, '%Y-%m') <> mes_archivo)                 AS fecha_fuera_del_mes,
       COUNT(*) FILTER (WHERE NOT (dropoff > pickup
                                   AND dropoff - pickup <= INTERVAL 24 HOUR))           AS duracion_invalida,
       COUNT(*) FILTER (WHERE NOT (trip_distance > 0 AND trip_distance <= 100))         AS distancia_invalida,
       COUNT(*) FILTER (WHERE NOT (fare_amount >= 0 AND total_amount > 0))              AS monto_invalido
FROM viajes
GROUP BY tipo
ORDER BY tipo DESC;


-- P1: Como evoluciona la cantidad de viajes por mes, por tipo de taxi y en conjunto
SELECT strftime(pickup, '%Y-%m')                    AS mes,
       COUNT(*) FILTER (WHERE tipo = 'yellow')      AS yellow,
       COUNT(*) FILTER (WHERE tipo = 'green')       AS green,
       COUNT(*)                                     AS total,
       ROUND(100.0 * COUNT(*) FILTER (WHERE tipo = 'green') / COUNT(*), 2) AS pct_green
FROM viajes_limpios
GROUP BY mes
ORDER BY mes;


-- P2: En que dias de la semana y horas del dia se concentra la demanda de viajes
SELECT isodow(pickup)                                                       AS num_dia,
       ['Lunes', 'Martes', 'Miercoles', 'Jueves', 'Viernes', 'Sabado', 'Domingo'][isodow(pickup)]
                                                                            AS dia,
       ROUND(COUNT(*) / COUNT(DISTINCT pickup::DATE))                       AS viajes_por_dia,
       ROUND(COUNT(*) FILTER (WHERE tipo = 'yellow') / COUNT(DISTINCT pickup::DATE)) AS yellow_por_dia,
       ROUND(COUNT(*) FILTER (WHERE tipo = 'green') / COUNT(DISTINCT pickup::DATE))  AS green_por_dia
FROM viajes_limpios
GROUP BY num_dia, dia
ORDER BY num_dia;


-- P2: En que dias de la semana y horas del dia se concentra la demanda de viajes (detalle)
SELECT hour(pickup) AS hora,
       ROUND(100 * COUNT(*) FILTER (WHERE tipo = 'yellow')
             / SUM(COUNT(*) FILTER (WHERE tipo = 'yellow')) OVER (), 2) AS pct_yellow,
       ROUND(100 * COUNT(*) FILTER (WHERE tipo = 'green')
             / SUM(COUNT(*) FILTER (WHERE tipo = 'green')) OVER (), 2)  AS pct_green
FROM viajes_limpios
GROUP BY hora
ORDER BY hora;


-- P3: Cual es la distancia y duracion tipica de los viajes, y como cambian entre meses
SELECT strftime(pickup, '%Y-%m')          AS mes,
       ROUND(AVG(trip_distance), 2)       AS distancia_promedio,
       ROUND(MEDIAN(trip_distance), 2)    AS distancia_mediana,
       ROUND(AVG(duracion_min), 1)        AS duracion_promedio_min,
       ROUND(MEDIAN(duracion_min), 1)     AS duracion_mediana_min
FROM viajes_limpios
GROUP BY mes
ORDER BY mes;


-- P4: Como evoluciona la tarifa base junto con los recargos adicionales a lo largo de los meses
SELECT tipo,
       strftime(pickup, '%Y-%m')                 AS mes,
       ROUND(AVG(fare_amount), 2)                AS tarifa_base,
       ROUND(AVG(extra), 2)                      AS extra,
       ROUND(AVG(congestion_surcharge), 2)       AS congestion,
       ROUND(AVG(cbd_congestion_fee), 2)         AS zona_congestion,
       ROUND(AVG(total_amount), 2)               AS total
FROM viajes_limpios
WHERE NOT sin_metadatos
GROUP BY tipo, mes
ORDER BY tipo DESC, mes;


-- P5: En que tipo de taxi se recorre una mayor distancia por viaje
SELECT tipo,
       ROUND(AVG(trip_distance), 2)                  AS distancia_promedio,
       ROUND(MEDIAN(trip_distance), 2)               AS distancia_mediana,
       ROUND(quantile_cont(trip_distance, 0.9), 2)   AS distancia_p90,
       ROUND(AVG(duracion_min), 1)                   AS duracion_promedio_min
FROM viajes_limpios
GROUP BY tipo
ORDER BY tipo DESC;


-- P6: Como se comparan los montos pagados en taxis amarillos y verdes, y que tan representativo es el monto maximo de cada uno
SELECT tipo,
       ROUND(AVG(total_amount), 2)                   AS promedio,
       ROUND(MEDIAN(total_amount), 2)                AS mediana,
       ROUND(quantile_cont(total_amount, 0.9), 2)    AS p90,
       ROUND(quantile_cont(total_amount, 0.99), 2)   AS p99,
       MAX(total_amount)                             AS maximo,
       COUNT(*) FILTER (WHERE total_amount > 500)    AS viajes_mayores_500
FROM viajes_limpios
GROUP BY tipo
ORDER BY tipo DESC;


-- P6: Como se comparan los montos pagados en taxis amarillos y verdes, y que tan representativo es el monto maximo de cada uno (detalle)
SELECT tipo, pickup, dropoff, trip_distance, RatecodeID, payment_type, fare_amount, total_amount
FROM viajes_limpios
ORDER BY total_amount DESC
LIMIT 6;


-- P7: Como se distribuyen las formas de pago en cada tipo de taxi
SELECT tipo,
       CASE payment_type
           WHEN 0 THEN 'Flex Fare'
           WHEN 1 THEN 'Tarjeta'
           WHEN 2 THEN 'Efectivo'
           WHEN 3 THEN 'Sin cargo'
           WHEN 4 THEN 'Disputa'
           WHEN 5 THEN 'Desconocido'
           WHEN 6 THEN 'Viaje anulado'
           ELSE 'Sin dato'
       END                                                              AS forma_de_pago,
       COUNT(*)                                                         AS viajes,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY tipo), 2) AS porcentaje
FROM viajes_limpios
GROUP BY tipo, forma_de_pago
ORDER BY tipo DESC, viajes DESC;


-- P8: Como evoluciona la propina por mes y en que mes se dan las propinas mas altas
SELECT strftime(pickup, '%Y-%m')                                            AS mes,
       ROUND(AVG(tip_amount) FILTER (WHERE tipo = 'yellow'), 2)             AS propina_yellow,
       ROUND(AVG(tip_amount) FILTER (WHERE tipo = 'green'), 2)              AS propina_green,
       ROUND(100 * AVG(tip_amount / fare_amount) FILTER (WHERE fare_amount > 0), 2)
                                                                            AS propina_pct_tarifa,
       ROUND(100 * AVG((tip_amount > 0)::INT), 1)                           AS pct_con_propina
FROM viajes_limpios
WHERE payment_type = 1  -- tarjeta: las propinas en efectivo no se registran
GROUP BY mes
ORDER BY mes;


-- P9: Que tan frecuentes son los cargos especiales y cuanto aportan al monto total
SELECT tipo,
       ROUND(100 * AVG((extra > 0)::INT), 1)                         AS pct_extra,
       ROUND(AVG(extra) FILTER (WHERE extra > 0), 2)                 AS monto_extra,
       ROUND(100 * AVG((congestion_surcharge > 0)::INT), 1)          AS pct_congestion,
       ROUND(AVG(congestion_surcharge) FILTER (WHERE congestion_surcharge > 0), 2) AS monto_congestion,
       ROUND(100 * AVG((cbd_congestion_fee > 0)::INT), 1)            AS pct_zona_congestion,
       ROUND(AVG(cbd_congestion_fee) FILTER (WHERE cbd_congestion_fee > 0), 2)     AS monto_zona_congestion,
       ROUND(100 * AVG((Airport_fee > 0)::INT), 1)                   AS pct_aeropuerto,
       ROUND(AVG(Airport_fee) FILTER (WHERE Airport_fee > 0), 2)     AS monto_aeropuerto,
       ROUND(100 * SUM(COALESCE(extra, 0) + COALESCE(congestion_surcharge, 0)
                       + COALESCE(cbd_congestion_fee, 0) + COALESCE(Airport_fee, 0))
             / SUM(total_amount), 1)                                 AS pct_del_total
FROM viajes_limpios
GROUP BY tipo
ORDER BY tipo DESC;


-- P10: Distribucion de distancia, tarifa y propina (percentiles)
SELECT 'distancia (millas)' AS variable,
       ROUND(quantile_cont(trip_distance, 0.25), 2) AS p25, ROUND(quantile_cont(trip_distance, 0.50), 2) AS p50,
       ROUND(quantile_cont(trip_distance, 0.75), 2) AS p75, ROUND(quantile_cont(trip_distance, 0.95), 2) AS p95,
       ROUND(quantile_cont(trip_distance, 0.99), 2) AS p99, ROUND(AVG(trip_distance), 2) AS promedio
FROM viajes_limpios
UNION ALL
SELECT 'tarifa (dolares)',
       ROUND(quantile_cont(fare_amount, 0.25), 2), ROUND(quantile_cont(fare_amount, 0.50), 2),
       ROUND(quantile_cont(fare_amount, 0.75), 2), ROUND(quantile_cont(fare_amount, 0.95), 2),
       ROUND(quantile_cont(fare_amount, 0.99), 2), ROUND(AVG(fare_amount), 2)
FROM viajes_limpios
WHERE NOT sin_metadatos
UNION ALL
SELECT 'propina con tarjeta (dolares)',
       ROUND(quantile_cont(tip_amount, 0.25), 2), ROUND(quantile_cont(tip_amount, 0.50), 2),
       ROUND(quantile_cont(tip_amount, 0.75), 2), ROUND(quantile_cont(tip_amount, 0.95), 2),
       ROUND(quantile_cont(tip_amount, 0.99), 2), ROUND(AVG(tip_amount), 2)
FROM viajes_limpios
WHERE payment_type = 1;


-- P11: Que mes genero el mayor y el menor monto total cobrado, y que lo explica
SELECT strftime(pickup, '%Y-%m')                                       AS mes,
       ROUND(SUM(total_amount) / 1e6, 2)                               AS total_millones,
       COUNT(*)                                                        AS viajes,
       COUNT(DISTINCT pickup::DATE)                                    AS dias,
       ROUND(SUM(total_amount) / COUNT(DISTINCT pickup::DATE) / 1e6, 3) AS millones_por_dia,
       ROUND(AVG(total_amount), 2)                                     AS monto_promedio
FROM viajes_limpios
GROUP BY mes
ORDER BY mes;


-- P12: Que proporcion de viajes presenta valores atipicos o inconsistentes y como cambian los promedios al excluirlos
SELECT 'sin limpiar'                                  AS conjunto,
       COUNT(*)                                       AS viajes,
       ROUND(AVG(trip_distance), 2)                   AS distancia_promedio,
       ROUND(AVG(epoch(dropoff - pickup) / 60), 1)    AS duracion_promedio_min,
       ROUND(AVG(total_amount), 2)                    AS total_promedio
FROM viajes
UNION ALL
SELECT 'limpio',
       COUNT(*),
       ROUND(AVG(trip_distance), 2),
       ROUND(AVG(duracion_min), 1),
       ROUND(AVG(total_amount), 2)
FROM viajes_limpios;

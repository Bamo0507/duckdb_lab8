-- CC3084 - Lab 8: DuckDB
-- Ejercicio 3 - Consultas directas sobre archivos Parquet
--
-- Todas las consultas leen directamente los archivos Parquet de data/raw/, sin
-- importarlos a una tabla. Las rutas son relativas a la raiz del proyecto
-- (/workspace dentro del contenedor lab). En notebooks/ejercicio_3.ipynb se
-- ejecutan las mismas consultas con rutas relativas a notebooks/ (../data/raw).
--
-- Fuentes:
--   data/raw/yellow/*/*.parquet   taxis amarillos
--   data/raw/green/*/*.parquet    taxis verdes
--   data/raw/*/*/*.parquet        ambos tipos


-- Q1 (3.1) Cantidad de archivos disponibles por tipo de taxi
SELECT regexp_extract(file, '(yellow|green)_tripdata', 1) AS tipo,
       COUNT(*)                                          AS archivos
FROM glob('data/raw/*/*/*.parquet')
GROUP BY tipo
ORDER BY tipo DESC;


-- Q2 (3.2) Cantidad de registros disponibles por tipo de taxi
SELECT regexp_extract(filename, '(yellow|green)_tripdata', 1) AS tipo,
       COUNT(*)                                              AS registros
FROM read_parquet('data/raw/*/*/*.parquet', filename = true, union_by_name = true)
GROUP BY tipo
ORDER BY tipo DESC;


-- Q3 (3.3 y 3.4) Columnas y tipos de datos de cada tipo de taxi
DESCRIBE SELECT * FROM read_parquet('data/raw/yellow/*/*.parquet');
DESCRIBE SELECT * FROM read_parquet('data/raw/green/*/*.parquet');


-- Q4 (3.4) Tamanio en disco de columnas categoricas vs tamanio estimado en memoria
-- 30040469 = total de registros obtenido en Q2; 8 bytes por valor BIGINT
SELECT path_in_schema                                     AS columna,
       ROUND(SUM(total_compressed_size) / 1024 ** 2, 1)   AS mb_en_disco,
       ROUND(30040469 * 8 / 1024 ** 2, 1)                 AS mb_en_memoria_bigint
FROM parquet_metadata('data/raw/*/*/*.parquet')
WHERE path_in_schema IN ('payment_type', 'RatecodeID', 'passenger_count')
GROUP BY path_in_schema
ORDER BY path_in_schema;


-- Q5 (3.5) Muestra reproducible de 5 registros por tipo de taxi (semilla 123)
SELECT * EXCLUDE (filename, file_row_number)
FROM (
    (SELECT 'yellow' AS tipo, *
     FROM read_parquet('data/raw/yellow/*/*.parquet', filename = true, file_row_number = true)
     ORDER BY hash(filename, file_row_number, 123)
     LIMIT 5)
    UNION ALL BY NAME
    (SELECT 'green' AS tipo, *
     FROM read_parquet('data/raw/green/*/*.parquet', filename = true, file_row_number = true)
     ORDER BY hash(filename, file_row_number, 123)
     LIMIT 5)
)
ORDER BY tipo DESC, COALESCE(tpep_pickup_datetime, lpep_pickup_datetime);


-- Q6 (3.6) Vista que unifica amarillos y verdes sobre los Parquet
CREATE OR REPLACE VIEW viajes AS
SELECT regexp_extract(filename, '(yellow|green)_tripdata', 1)         AS tipo,
       regexp_extract(filename, '_(\d{4}-\d{2})\.parquet', 1)          AS mes_archivo,
       COALESCE(tpep_pickup_datetime, lpep_pickup_datetime)            AS pickup,
       COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime)          AS dropoff,
       *
FROM read_parquet('data/raw/*/*/*.parquet', filename = true, union_by_name = true);


-- Q7 (3.6) Conteo de problemas de calidad de datos por tipo de taxi
SELECT tipo,
       COUNT(*)                                                                    AS total,
       COUNT(*) FILTER (WHERE passenger_count IS NULL AND RatecodeID IS NULL
                          AND store_and_fwd_flag IS NULL AND congestion_surcharge IS NULL)
                                                                                   AS bloque_incompleto,
       COUNT(*) FILTER (WHERE strftime(pickup, '%Y-%m') <> mes_archivo)            AS fuera_del_mes,
       COUNT(*) FILTER (WHERE year(pickup) <> 2026)                                AS fuera_de_2026,
       COUNT(*) FILTER (WHERE dropoff < pickup)                                    AS termina_antes_de_iniciar,
       COUNT(*) FILTER (WHERE dropoff = pickup)                                    AS duracion_cero,
       COUNT(*) FILTER (WHERE dropoff - pickup > INTERVAL 24 HOUR)                 AS duracion_mayor_24h,
       COUNT(*) FILTER (WHERE trip_distance = 0)                                   AS distancia_cero,
       COUNT(*) FILTER (WHERE trip_distance > 100)                                 AS distancia_mayor_100,
       COUNT(*) FILTER (WHERE fare_amount < 0)                                     AS tarifa_negativa,
       COUNT(*) FILTER (WHERE total_amount < 0)                                    AS total_negativo,
       COUNT(*) FILTER (WHERE passenger_count = 0)                                 AS pasajeros_cero,
       COUNT(*) FILTER (WHERE RatecodeID = 99)                                     AS ratecode_99,
       COUNT(*) FILTER (WHERE PULocationID IN (264, 265) OR DOLocationID IN (264, 265))
                                                                                   AS zona_desconocida,
       COUNT(*) FILTER (WHERE tipo = 'green' AND ehail_fee IS NOT NULL)            AS ehail_fee_con_valor
FROM viajes
GROUP BY tipo
ORDER BY tipo DESC;


-- Q8 (3.6) Evidencia del bloque incompleto: nulos simultaneos y payment_type
SELECT tipo,
       passenger_count IS NULL AS sin_datos,
       payment_type,
       COUNT(*)                AS registros
FROM viajes
WHERE passenger_count IS NULL OR payment_type = 0
GROUP BY ALL
ORDER BY tipo DESC, payment_type;


-- Q9 (3.6) Evidencia de valores extremos en fechas, distancias y montos
SELECT tipo,
       MIN(pickup)        AS pickup_minimo,
       MAX(pickup)        AS pickup_maximo,
       MAX(trip_distance) AS distancia_maxima,
       MIN(total_amount)  AS total_minimo,
       MAX(total_amount)  AS total_maximo
FROM viajes
GROUP BY tipo
ORDER BY tipo DESC;


-- Q10 (3.9) Viajes de amarillos y verdes en un periodo (marzo 2026):
-- solo se leen las columnas de fecha de inicio
SELECT regexp_extract(filename, '(yellow|green)_tripdata', 1) AS tipo,
       COUNT(*)                                              AS viajes
FROM read_parquet('data/raw/*/*/*.parquet', filename = true, union_by_name = true)
WHERE COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) >= TIMESTAMP '2026-03-01'
  AND COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) <  TIMESTAMP '2026-04-01'
GROUP BY tipo
ORDER BY tipo DESC;

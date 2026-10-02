-- CC3084 - Lab 8: DuckDB
-- Ejercicio 5 - Incorporacion de datos adicionales (2024)
--
-- Consultas utilizadas para validar la incorporacion de 2024. Todas leen
-- directamente los archivos Parquet; las rutas son relativas a la raiz del
-- proyecto (/workspace dentro del contenedor lab). En notebooks/ejercicio_5.ipynb
-- se ejecutan con rutas relativas a notebooks/ (../data/raw).


-- Q1 (5.5) Cobertura de 2024: registros por archivo y porcentaje fuera de su mes
WITH viajes AS (
    SELECT regexp_extract(filename, '(yellow|green)_tripdata', 1)        AS tipo,
           regexp_extract(filename, '_(\d{4}-\d{2})\.parquet', 1)         AS mes_archivo,
           strftime(COALESCE(tpep_pickup_datetime, lpep_pickup_datetime), '%Y-%m') AS mes_viaje
    FROM read_parquet('data/raw/*/2024/*.parquet', filename = true, union_by_name = true)
)
SELECT tipo,
       mes_archivo,
       COUNT(*)                                                                      AS viajes,
       ROUND(100.0 * COUNT(*) FILTER (WHERE mes_viaje <> mes_archivo) / COUNT(*), 4) AS pct_fuera_del_mes
FROM viajes
GROUP BY tipo, mes_archivo
ORDER BY tipo DESC, mes_archivo;


-- Q2 (5.6) Consulta conjunta de 2024 y 2026 por anio y tipo de taxi
SELECT regexp_extract(filename, '_(\d{4})-', 1)                      AS anio,
       regexp_extract(filename, '(yellow|green)_tripdata', 1)        AS tipo,
       COUNT(*)                                                      AS viajes,
       COUNT(DISTINCT regexp_extract(filename, '_(\d{4}-\d{2})\.parquet', 1)) AS meses,
       ROUND(COUNT(*) / COUNT(DISTINCT regexp_extract(filename, '_(\d{4}-\d{2})\.parquet', 1))) AS viajes_por_mes,
       ROUND(MEDIAN(trip_distance), 2)                               AS distancia_mediana,
       ROUND(MEDIAN(total_amount), 2)                                AS total_mediano
FROM read_parquet('data/raw/*/*/*.parquet', filename = true, union_by_name = true)
GROUP BY anio, tipo
ORDER BY anio, tipo DESC;


-- Q3 (5.7) Columnas que no existen en todos los archivos de un mismo tipo de taxi
WITH esquema AS (
    SELECT regexp_extract(file_name, '(yellow|green)_tripdata', 1) AS tipo,
           regexp_extract(file_name, '_(\d{4})-', 1)              AS anio,
           file_name,
           name                                                   AS columna
    FROM parquet_schema('data/raw/*/*/*.parquet')
    WHERE name <> 'schema'
),
archivos_por_tipo AS (
    SELECT tipo, COUNT(DISTINCT file_name) AS archivos_totales
    FROM esquema
    GROUP BY tipo
)
SELECT e.tipo,
       e.columna,
       string_agg(DISTINCT e.anio, ', ' ORDER BY e.anio)  AS anios_con_la_columna,
       COUNT(DISTINCT e.file_name)                        AS archivos_con_la_columna,
       a.archivos_totales
FROM esquema e
JOIN archivos_por_tipo a USING (tipo)
GROUP BY e.tipo, e.columna, a.archivos_totales
HAVING COUNT(DISTINCT e.file_name) < a.archivos_totales
ORDER BY e.tipo DESC, e.columna;


-- Q4 (5.7) Vistas del Ejercicio 4, sin cambios, sobre todos los anios descargados
CREATE OR REPLACE VIEW viajes AS
SELECT regexp_extract(filename, '(yellow|green)_tripdata', 1)         AS tipo,
       regexp_extract(filename, '_(\d{4}-\d{2})\.parquet', 1)          AS mes_archivo,
       COALESCE(tpep_pickup_datetime, lpep_pickup_datetime)            AS pickup,
       COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime)          AS dropoff,
       passenger_count IS NULL AND RatecodeID IS NULL                  AS sin_metadatos,
       *
FROM read_parquet('data/raw/*/*/*.parquet', filename = true, union_by_name = true);

CREATE OR REPLACE VIEW viajes_limpios AS
SELECT *,
       epoch(dropoff - pickup) / 60 AS duracion_min
FROM viajes
WHERE strftime(pickup, '%Y-%m') = mes_archivo
  AND dropoff > pickup AND dropoff - pickup <= INTERVAL 24 HOUR
  AND trip_distance > 0 AND trip_distance <= 100
  AND fare_amount >= 0 AND total_amount > 0;

SELECT year(pickup)                                            AS anio,
       COUNT(*)                                                AS viajes_limpios,
       ROUND(100 * AVG(sin_metadatos::INT), 1)                 AS pct_sin_metadatos,
       ROUND(MEDIAN(trip_distance), 2)                         AS distancia_mediana,
       ROUND(MEDIAN(duracion_min), 1)                          AS duracion_mediana_min
FROM viajes_limpios
GROUP BY anio
ORDER BY anio;

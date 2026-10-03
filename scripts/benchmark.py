#!/usr/bin/env python3
"""Benchmark: consultas sobre archivos Parquet vs tabla materializada en DuckDB.

Ejecuta el mismo conjunto de consultas representativas del analisis (Ejercicio 4)
sobre dos fuentes equivalentes:

  - viajes_parquet : vista que lee directamente los archivos Parquet de data/raw/
  - viajes_tabla   : tabla materializada en data/processed/taxis.duckdb

Uso:
    python scripts/benchmark.py                          # todos los anios descargados
    python scripts/benchmark.py --anio 2026              # solo un anio
    python scripts/benchmark.py --anio 2024 2026 --repeticiones 5

Los resultados se guardan en data/processed/benchmark_<anios>.csv.

Metodologia:
  - Ambas fuentes exponen las mismas columnas, por lo que cada consulta se
    ejecuta con el mismo texto SQL cambiando unicamente la fuente.
  - Cada consulta se ejecuta una vez como calentamiento (no se mide) y luego
    `repeticiones` veces; se reporta la mediana de los tiempos.
  - Ambas fuentes se consultan desde la misma conexion y con la misma
    configuracion de DuckDB.
"""

import argparse
import statistics
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd

RAIZ = Path(__file__).resolve().parent.parent
DIR_RAW = RAIZ / "data" / "raw"
DIR_PROCESSED = RAIZ / "data" / "processed"
RUTA_DB = DIR_PROCESSED / "taxis.duckdb"

REPETICIONES = 3
CALENTAMIENTO = 1

# Limite de memoria de DuckDB: el contenedor comparte la RAM con Metabase, por lo
# que sin limite la materializacion puede agotarla. Lo que no cabe se escribe en disco.
MEMORIA_MAXIMA = "2GB"
DIR_TEMPORAL = DIR_PROCESSED / "tmp_duckdb"

# Columnas derivadas que se agregan a los datos crudos (mismas del Ejercicio 4)
SQL_VIAJES = """
    SELECT regexp_extract(filename, '(yellow|green)_tripdata', 1)         AS tipo,
           regexp_extract(filename, '_(\\d{{4}}-\\d{{2}})\\.parquet', 1)    AS mes_archivo,
           COALESCE(tpep_pickup_datetime, lpep_pickup_datetime)            AS pickup,
           COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime)          AS dropoff,
           passenger_count IS NULL AND RatecodeID IS NULL                  AS sin_metadatos,
           * EXCLUDE (filename)
    FROM read_parquet({rutas}, filename = true, union_by_name = true)
"""

# Filtro de viajes validos (vista viajes_limpios del Ejercicio 4)
FILTRO_LIMPIO = """
    strftime(pickup, '%Y-%m') = mes_archivo
    AND dropoff > pickup AND dropoff - pickup <= INTERVAL 24 HOUR
    AND trip_distance > 0 AND trip_distance <= 100
    AND fare_amount >= 0 AND total_amount > 0
"""

# Consultas representativas del analisis; {fuente} se reemplaza por la vista o la tabla.
# Las medianas y percentiles usan approx_quantile: con todos los anios, MEDIAN y
# quantile_cont guardan todos los valores en memoria y agotan la RAM del contenedor.
CONSULTAS = {
    "C1 - Conteo total de viajes": """
        SELECT COUNT(*) FROM {fuente}
    """,
    "C2 - Viajes por mes y tipo (P1)": f"""
        SELECT strftime(pickup, '%Y-%m') AS mes, tipo, COUNT(*) AS viajes
        FROM {{fuente}}
        WHERE {FILTRO_LIMPIO}
        GROUP BY mes, tipo
        ORDER BY mes, tipo
    """,
    "C3 - Viajes por hora del dia (P2)": f"""
        SELECT hour(pickup) AS hora, COUNT(*) AS viajes
        FROM {{fuente}}
        WHERE {FILTRO_LIMPIO}
        GROUP BY hora
        ORDER BY hora
    """,
    "C4 - Distancia y duracion mediana por mes (P3)": f"""
        SELECT strftime(pickup, '%Y-%m') AS mes,
               approx_quantile(trip_distance, 0.5) AS distancia_mediana,
               approx_quantile(epoch(dropoff - pickup) / 60, 0.5) AS duracion_mediana
        FROM {{fuente}}
        WHERE {FILTRO_LIMPIO}
        GROUP BY mes
        ORDER BY mes
    """,
    "C5 - Percentiles del monto por tipo (P6)": f"""
        SELECT tipo,
               approx_quantile(total_amount, [0.5, 0.9, 0.99]) AS percentiles,
               MAX(total_amount) AS maximo
        FROM {{fuente}}
        WHERE {FILTRO_LIMPIO}
        GROUP BY tipo
    """,
    "C6 - Formas de pago por tipo (P7)": f"""
        SELECT tipo, payment_type, COUNT(*) AS viajes
        FROM {{fuente}}
        WHERE {FILTRO_LIMPIO}
        GROUP BY tipo, payment_type
        ORDER BY tipo, viajes DESC
    """,
    "C7 - Propina promedio con tarjeta por mes (P8)": f"""
        SELECT strftime(pickup, '%Y-%m') AS mes, tipo, AVG(tip_amount) AS propina
        FROM {{fuente}}
        WHERE {FILTRO_LIMPIO} AND payment_type = 1
        GROUP BY mes, tipo
        ORDER BY mes, tipo
    """,
    "C8 - Monto total cobrado por mes (P11)": f"""
        SELECT strftime(pickup, '%Y-%m') AS mes, SUM(total_amount) AS total
        FROM {{fuente}}
        WHERE {FILTRO_LIMPIO}
        GROUP BY mes
        ORDER BY mes
    """,
}


def conectar(ruta: Path = RUTA_DB):
    """Conexion a la base DuckDB con la configuracion del benchmark."""
    con = duckdb.connect(str(ruta))
    con.execute(f"SET memory_limit = '{MEMORIA_MAXIMA}'")
    con.execute(f"SET temp_directory = '{DIR_TEMPORAL}'")
    con.execute("SET preserve_insertion_order = false")
    return con


def anios_descargados() -> list:
    """Anios que tienen al menos un archivo Parquet en data/raw/<tipo>/<anio>/."""
    return sorted({int(p.parent.name) for p in DIR_RAW.glob("*/*/*.parquet")})


def rutas_parquet(anios) -> str:
    """Lista SQL con un patron por anio, p. ej. ['.../*/2024/*.parquet', ...]."""
    patrones = [f"'{DIR_RAW}/*/{anio}/*.parquet'" for anio in anios]
    return "[" + ", ".join(patrones) + "]"


def crear_vista_parquet(con, anios, nombre: str = "viajes_parquet") -> None:
    """Vista que consulta directamente los archivos Parquet de los anios indicados."""
    con.execute(f"CREATE OR REPLACE VIEW {nombre} AS {SQL_VIAJES.format(rutas=rutas_parquet(anios))}")


def materializar(con, anios, nombre: str = "viajes_tabla") -> float:
    """Crea la tabla materializada a partir de los Parquet. Devuelve los segundos que tomo."""
    crear_vista_parquet(con, anios, "_origen_materializacion")
    inicio = time.perf_counter()
    con.execute(f"CREATE OR REPLACE TABLE {nombre} AS SELECT * FROM _origen_materializacion")
    duracion = time.perf_counter() - inicio
    con.execute("DROP VIEW _origen_materializacion")
    con.execute("CHECKPOINT")
    return duracion


def medir(con, sql: str, repeticiones: int = REPETICIONES, calentamiento: int = CALENTAMIENTO) -> float:
    """Mediana en segundos de `repeticiones` ejecuciones, despues del calentamiento."""
    for _ in range(calentamiento):
        con.execute(sql).fetchall()
    tiempos = []
    for _ in range(repeticiones):
        inicio = time.perf_counter()
        con.execute(sql).fetchall()
        tiempos.append(time.perf_counter() - inicio)
    return statistics.median(tiempos)


def ejecutar_benchmark(con, fuente_parquet: str = "viajes_parquet", fuente_tabla: str = "viajes_tabla",
                       repeticiones: int = REPETICIONES) -> pd.DataFrame:
    """Ejecuta todas las consultas sobre ambas fuentes y devuelve los tiempos."""
    filas = []
    for nombre, plantilla in CONSULTAS.items():
        t_parquet = medir(con, plantilla.format(fuente=fuente_parquet), repeticiones)
        t_tabla = medir(con, plantilla.format(fuente=fuente_tabla), repeticiones)
        filas.append({
            "consulta": nombre,
            "tiempo_parquet_s": round(t_parquet, 3),
            "tiempo_tabla_s": round(t_tabla, 3),
            "aceleracion_tabla": round(t_parquet / t_tabla, 2),
        })
        print(f"  {nombre:<50} parquet {t_parquet:7.3f} s   tabla {t_tabla:7.3f} s")
    return pd.DataFrame(filas)


def ejecutar_escenario(con, anios, repeticiones: int = REPETICIONES, conservar_tabla: bool = False) -> pd.DataFrame:
    """Benchmark completo para un conjunto de anios: crea su vista y su tabla y mide las consultas.

    Por defecto la tabla del escenario se elimina al terminar para no ocupar disco.
    """
    etiqueta = "_".join(map(str, sorted(anios)))
    vista, tabla = f"viajes_parquet_{etiqueta}", f"viajes_tabla_{etiqueta}"
    crear_vista_parquet(con, anios, vista)
    segundos = materializar(con, anios, tabla)
    registros = con.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
    print(f"Escenario {etiqueta}: {registros:,} registros, tabla creada en {segundos:.1f} s")

    resultados = ejecutar_benchmark(con, vista, tabla, repeticiones)
    resultados.insert(0, "escenario", etiqueta)
    resultados.insert(1, "registros", registros)
    resultados.insert(2, "creacion_tabla_s", round(segundos, 1))

    if not conservar_tabla:
        con.execute(f"DROP TABLE {tabla}")
        con.execute(f"DROP VIEW {vista}")
        con.execute("CHECKPOINT")
    return resultados


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark Parquet vs tabla materializada en DuckDB.")
    parser.add_argument("--anio", type=int, nargs="+", help="anios a incluir (por defecto: todos los descargados)")
    parser.add_argument("--repeticiones", type=int, default=REPETICIONES, help="ejecuciones medidas por consulta")
    argumentos = parser.parse_args()

    anios = sorted(set(argumentos.anio)) if argumentos.anio else anios_descargados()
    if not anios:
        print("No hay archivos en data/raw/. Ejecute primero scripts/download_data.py")
        return 1

    DIR_PROCESSED.mkdir(parents=True, exist_ok=True)
    etiqueta = "_".join(map(str, anios))
    with conectar() as con:
        crear_vista_parquet(con, anios)
        print(f"Materializando la tabla con {etiqueta} ...")
        segundos = materializar(con, anios)
        registros = con.execute("SELECT COUNT(*) FROM viajes_tabla").fetchone()[0]
        print(f"  {registros:,} registros en {segundos:.1f} s")

        print("Ejecutando consultas ...")
        resultados = ejecutar_benchmark(con, repeticiones=argumentos.repeticiones)

    resultados.insert(0, "anios", etiqueta)
    resultados.insert(1, "registros", registros)
    salida = DIR_PROCESSED / f"benchmark_{etiqueta}.csv"
    resultados.to_csv(salida, index=False)
    print(f"Resultados guardados en {salida}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

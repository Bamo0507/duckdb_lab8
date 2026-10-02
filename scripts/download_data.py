#!/usr/bin/env python3
"""Descarga los archivos Parquet del NYC TLC Trip Record Data.

Descarga los registros de viajes de taxis amarillos (yellow) y verdes (green)
de los anios indicados. Por defecto descarga 2026, que es el conjunto de datos
inicial del laboratorio.

Fuente oficial de los datos:
    https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Uso:
    python scripts/download_data.py                       # amarillos y verdes de 2026
    python scripts/download_data.py --taxi yellow
    python scripts/download_data.py --anio 2024 2025 2026

Los archivos se guardan en:
    data/raw/<tipo>/<anio>/<nombre-original>.parquet

La ruta se resuelve a partir de la ubicacion de este script, por lo que el
resultado es el mismo sin importar desde que directorio se ejecute.

Comportamiento:
  - La TLC publica cada mes con varias semanas de atraso, por lo que no todos
    los meses del anio en curso existen todavia. El script consulta al servidor
    que meses estan publicados en lugar de suponerlos.
  - Se distingue un mes no publicado (el servidor responde 403/404) de un
    error de red o del servidor; este ultimo se reporta como fallido.
  - Un archivo que ya existe localmente no se vuelve a descargar.
  - La descarga se hace sobre un nombre temporal y solo se renombra al
    terminar, de modo que una interrupcion no deja archivos .parquet a medias.
  - Antes de renombrar se verifica que el tamanio coincida con el informado por
    el servidor (Content-Length) y que el archivo sea un Parquet legible.
  - Los reintentos esperan un tiempo creciente entre intentos y se hace una
    pausa entre peticiones para no saturar al servidor.
"""

import argparse
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq
import requests

ANIOS_POR_DEFECTO = (2026,)
TIPOS_TAXI = ("yellow", "green")
URL_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DIR_DESTINO = Path(__file__).resolve().parent.parent / "data" / "raw"

TIEMPO_ESPERA = 60          # segundos por peticion
INTENTOS = 3                # intentos por archivo antes de darse por vencido
ESPERA_BASE = 2             # segundos; la espera entre intentos se duplica (2, 4, ...)
PAUSA_ENTRE_PETICIONES = 1  # segundos entre un archivo y el siguiente
BLOQUE = 1024 * 1024        # 1 MiB por bloque de descarga
SUFIJO_TEMPORAL = ".part"

# La TLC sirve los archivos desde CloudFront/S3, que responde 403 (no 404)
# cuando el archivo no existe.
CODIGOS_NO_PUBLICADO = (403, 404)

PUBLICADO = "publicado"
NO_PUBLICADO = "no_publicado"
ERROR = "error"


def construir_nombre(tipo: str, anio: int, mes: int) -> str:
    """Nombre del archivo publicado por la TLC, p. ej. yellow_tripdata_2026-01.parquet."""
    return f"{tipo}_tripdata_{anio}-{mes:02d}.parquet"


def construir_url(tipo: str, anio: int, mes: int) -> str:
    """URL completa del archivo Parquet mensual."""
    return f"{URL_BASE}/{construir_nombre(tipo, anio, mes)}"


def ruta_destino(tipo: str, anio: int, mes: int) -> Path:
    """Ruta local donde se guarda el archivo."""
    return DIR_DESTINO / tipo / str(anio) / construir_nombre(tipo, anio, mes)


def esperar_reintento(intento: int) -> None:
    """Espera creciente entre intentos: ESPERA_BASE, 2*ESPERA_BASE, ..."""
    time.sleep(ESPERA_BASE * 2 ** (intento - 1))


def consultar_publicacion(url: str) -> tuple:
    """Consulta si el archivo existe en el servidor (sin descargarlo).

    Devuelve (estado, tamanio_esperado, detalle), donde estado es PUBLICADO,
    NO_PUBLICADO o ERROR. Los errores de red y del servidor se reintentan.
    """
    detalle = ""
    for intento in range(1, INTENTOS + 1):
        try:
            respuesta = requests.head(url, timeout=TIEMPO_ESPERA, allow_redirects=True)
        except requests.RequestException as error:
            detalle = f"error de red: {error}"
        else:
            if respuesta.ok:
                tamanio = respuesta.headers.get("Content-Length")
                return PUBLICADO, int(tamanio) if tamanio else None, ""
            if respuesta.status_code in CODIGOS_NO_PUBLICADO:
                return NO_PUBLICADO, None, ""
            detalle = f"el servidor respondio HTTP {respuesta.status_code}"
        if intento < INTENTOS:
            esperar_reintento(intento)
    return ERROR, None, detalle


def formato_tamanio(n: float) -> str:
    for unidad in ("B", "KiB", "MiB", "GiB"):
        if n < 1024 or unidad == "GiB":
            return f"{n:.1f} {unidad}"
        n /= 1024
    return f"{n:.1f} GiB"


def verificar_parquet(ruta: Path) -> None:
    """Falla si el archivo no es un Parquet legible (lee solo los metadatos)."""
    try:
        pq.ParquetFile(ruta).metadata
    except Exception as error:
        raise requests.RequestException(f"el archivo no es un Parquet valido: {error}")


def descargar_archivo(url: str, destino: Path, tamanio_esperado) -> int:
    """Descarga `url` en `destino` y verifica el resultado. Devuelve los bytes escritos."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporal = destino.with_name(destino.name + SUFIJO_TEMPORAL)

    ultimo_error = None
    for intento in range(1, INTENTOS + 1):
        try:
            with requests.get(url, stream=True, timeout=TIEMPO_ESPERA) as respuesta:
                respuesta.raise_for_status()
                escritos = 0
                with temporal.open("wb") as archivo:
                    for bloque in respuesta.iter_content(chunk_size=BLOQUE):
                        if bloque:
                            archivo.write(bloque)
                            escritos += len(bloque)
            if escritos == 0:
                raise requests.RequestException("el servidor devolvio un archivo vacio")
            if tamanio_esperado is not None and escritos != tamanio_esperado:
                raise requests.RequestException(
                    f"descarga incompleta: {escritos} de {tamanio_esperado} bytes"
                )
            verificar_parquet(temporal)
            temporal.replace(destino)
            return escritos
        except requests.RequestException as error:
            ultimo_error = error
            temporal.unlink(missing_ok=True)
            if intento < INTENTOS:
                print(f"      intento {intento}/{INTENTOS} fallido ({error}); reintentando")
                esperar_reintento(intento)

    raise requests.RequestException(f"no se pudo descargar {url}: {ultimo_error}")


def descargar(tipo: str, anio: int) -> dict:
    """Descarga todos los meses publicados de un tipo de taxi para un anio."""
    print(f"\n=== {tipo.upper()} {anio} ===")
    resumen = {"descargados": 0, "omitidos": 0, "no_publicados": [], "fallidos": []}

    for mes in range(1, 13):
        etiqueta = f"{anio}-{mes:02d}"
        destino = ruta_destino(tipo, anio, mes)

        if destino.exists() and destino.stat().st_size > 0:
            print(f"  {etiqueta}  ya existe, se omite")
            resumen["omitidos"] += 1
            continue

        url = construir_url(tipo, anio, mes)
        estado, tamanio_esperado, detalle = consultar_publicacion(url)
        if estado == NO_PUBLICADO:
            print(f"  {etiqueta}  aun no publicado por la TLC")
            resumen["no_publicados"].append(etiqueta)
            continue
        if estado == ERROR:
            print(f"  {etiqueta}  ERROR al consultar el servidor: {detalle}")
            resumen["fallidos"].append(etiqueta)
            continue

        print(f"  {etiqueta}  descargando...")
        try:
            escritos = descargar_archivo(url, destino, tamanio_esperado)
        except requests.RequestException as error:
            print(f"  {etiqueta}  ERROR: {error}")
            resumen["fallidos"].append(etiqueta)
        else:
            print(f"  {etiqueta}  listo ({formato_tamanio(escritos)}) -> {destino}")
            resumen["descargados"] += 1
        time.sleep(PAUSA_ENTRE_PETICIONES)

    return resumen


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Descarga los datos de taxis amarillos y verdes del NYC TLC."
    )
    parser.add_argument(
        "--taxi", choices=(*TIPOS_TAXI, "all"), default="all",
        help="tipo de taxi a descargar (por defecto: all)",
    )
    parser.add_argument(
        "--anio", type=int, nargs="+", default=list(ANIOS_POR_DEFECTO),
        help="uno o varios anios a descargar (por defecto: %(default)s)",
    )
    argumentos = parser.parse_args()

    tipos = TIPOS_TAXI if argumentos.taxi == "all" else (argumentos.taxi,)
    anios = sorted(set(argumentos.anio))

    total = {"descargados": 0, "omitidos": 0, "no_publicados": [], "fallidos": []}
    for anio in anios:
        for tipo in tipos:
            resumen = descargar(tipo, anio)
            total["descargados"] += resumen["descargados"]
            total["omitidos"] += resumen["omitidos"]
            total["no_publicados"] += [f"{tipo} {m}" for m in resumen["no_publicados"]]
            total["fallidos"] += [f"{tipo} {m}" for m in resumen["fallidos"]]

    print("\n" + "=" * 60)
    print("RESUMEN")
    print("=" * 60)
    print(f"  anios         : {', '.join(map(str, anios))}")
    print(f"  destino       : {DIR_DESTINO}")
    print(f"  descargados   : {total['descargados']}")
    print(f"  ya existian   : {total['omitidos']}")
    print(f"  no publicados : {len(total['no_publicados'])}")
    if total["no_publicados"]:
        print(f"      {', '.join(total['no_publicados'])}")
    print(f"  fallidos      : {len(total['fallidos'])}")
    if total["fallidos"]:
        print(f"      {', '.join(total['fallidos'])}")
    print("=" * 60)

    return 1 if total["fallidos"] else 0


if __name__ == "__main__":
    sys.exit(main())

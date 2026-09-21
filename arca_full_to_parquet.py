#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Convierte un mes completo de importaciones públicas de ARCA a Parquet.

Salida por período YYYYMM:
  data/items/YYYYMM.parquet
  data/taxes/YYYYMM.parquet
  metadata/months/YYYYMM.json

El parser conserva todas las operaciones válidas. Los conceptos tributarios se
separan de la tabla de ítems para evitar duplicar FOB/cantidades cuando ARCA
publica varias filas para un mismo ítem.
"""
from __future__ import annotations

import argparse
import hashlib
import html
from html.parser import HTMLParser
import json
import logging
import re
import shutil
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urljoin
from urllib.request import Request, urlopen

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


ARCA_INDEX_URLS = [
    "https://www.arca.gob.ar/operadoresComercioExterior/informacionAgregada/informacion-agregada.asp",
    "https://www.afip.gob.ar/operadoresComercioExterior/informacionAgregada/informacion-agregada.asp",
]

DOWNLOAD_URL_PATTERNS = [
    "https://www.arca.gob.ar/operadoresComercioExterior/informacionAgregada/download.aspx?filename={periodo}.zip",
    "https://www.afip.gob.ar/operadoresComercioExterior/informacionAgregada/download.aspx?filename={periodo}.zip",
]

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/153.0 Safari/537.36"
)

BATCH_SIZE = 50_000
DOWNLOAD_CHUNK = 4 * 1024 * 1024

UNIT_NAMES = {
    "01": "KILOGRAMO",
    "02": "METRO",
    "03": "METRO CUADRADO",
    "04": "METRO CUBICO",
    "05": "LITRO",
    "07": "UNIDAD",
    "08": "PAR",
    "09": "DOCENA",
    "11": "MILLAR",
    "14": "GRAMO",
    "21": "KILOGRAMO ACTIVO",
    "22": "GRAMO ACTIVO",
    "23": "GRAMO BASE",
    "25": "JUEGO/PAQUETE",
    "29": "TONELADA",
    "41": "MILIGRAMO",
    "47": "MILILITRO",
    "53": "KILOGRAMO BASE",
    "61": "KILOGRAMO BRUTO",
    "62": "PACK",
    "98": "OTRAS UNIDADES",
}

RAW_SCHEMA = pa.schema([
    ("periodo", pa.string()),
    ("anio", pa.int16()),
    ("mes", pa.int8()),
    ("aduana", pa.string()),
    ("destinacion", pa.string()),
    ("item", pa.string()),
    ("fecha", pa.string()),
    ("importador", pa.string()),
    ("medio_transporte", pa.string()),
    ("unidad", pa.string()),
    ("unidad_nombre", pa.string()),
    ("cantidad", pa.float64()),
    ("cantidad_kg", pa.float64()),
    ("valor_item_usd", pa.float64()),
    ("valor_destinacion_usd", pa.float64()),
    ("usd_por_unidad_declarada", pa.float64()),
    ("usd_por_kg", pa.float64()),
    ("divisa", pa.string()),
    ("pais_origen", pa.string()),
    ("pais_procedencia", pa.string()),
    ("posicion_arancelaria_declarada", pa.string()),
    ("ncm", pa.string()),
    ("concepto", pa.string()),
    ("monto_usd", pa.float64()),
    ("row_hash", pa.string()),
])


def setup_logging(verbose: bool) -> logging.Logger:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    return logging.getLogger("arca-full")


def safe_float(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", ".")
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(value or "").strip())


def normalize_header(value: str) -> str:
    value = normalize_text(value).upper()
    value = (
        value.replace("Á", "A")
        .replace("É", "E")
        .replace("Í", "I")
        .replace("Ó", "O")
        .replace("Ú", "U")
        .replace("Ñ", "N")
    )
    return re.sub(r"[^A-Z0-9]+", "_", value).strip("_")


def value_at(parts: list[str], index: int | None) -> str:
    if index is None or index < 0 or index >= len(parts):
        return ""
    return normalize_text(parts[index])


def quantity_to_kg(qty: float | None, unit: str) -> float | None:
    if qty is None:
        return None
    unit = (unit or "").strip()
    if unit in {"01", "21", "53"}:
        return qty
    if unit == "29":
        return qty * 1000.0
    if unit in {"14", "22", "23"}:
        return qty / 1000.0
    if unit == "41":
        return qty / 1_000_000.0
    return None


def sha1_bytes(data: bytes) -> str:
    return hashlib.sha1(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass
class Layout:
    aduana: int
    destinacion: int
    item: int
    fecha: int
    importador: int
    medio: int
    unidad: int
    cantidad: int
    valor_item: int
    valor_destinacion: int
    divisa: int | None
    pais_origen: int
    pais_procedencia: int
    posicion_declarada: int | None
    ncm: int
    concepto: int
    monto: int
    field_count: int
    header_normalized: list[str]


def find_index(headers: list[str], aliases: Iterable[str]) -> int | None:
    aliases = set(aliases)
    for i, header in enumerate(headers):
        if header in aliases:
            return i
    return None


def find_contains(headers: list[str], needles: Iterable[str]) -> int | None:
    for i, header in enumerate(headers):
        if any(needle in header for needle in needles):
            return i
    return None


def detect_layout(header_line: str, logger: logging.Logger) -> Layout:
    headers = [
        normalize_header(x)
        for x in header_line.rstrip("\r\n").split("'")
    ]

    aduana = find_index(headers, {"ADU", "ADUANA"})
    destinacion = find_contains(headers, {"DESTINACION"})
    item = find_contains(headers, {"NUM_ITEM", "ITEM"})
    fecha = find_contains(headers, {"FECHA"})
    importador = find_contains(headers, {"NOMBRE_IMPORTADOR", "IMPORTADOR"})
    medio = find_index(headers, {"M", "MEDIO", "MEDIO_TRANSPORTE"})
    unidad = find_index(headers, {"UN", "UNIDAD", "UNIDAD_MEDIDA"})
    cantidad = find_contains(headers, {"CANT_UNIDAD_MEDIDA", "CANTIDAD"})

    valor_item = find_index(
        headers,
        {"FOB_DOLAR", "VALOR_ITEM", "FOB_CIF_UNITARIO", "VALOR_FOB_CIF_UNITARIO"},
    )
    if valor_item is None:
        valor_item = find_contains(headers, {"FOB_DOLAR", "VALOR_ITEM"})

    valor_total = find_index(
        headers,
        {"FOB_TOTAL", "VALOR_TOTAL", "FOB_CIF_TOTAL", "VALOR_FOB_CIF_TOTAL"},
    )
    if valor_total is None:
        valor_total = find_contains(headers, {"FOB_TOTAL", "VALOR_TOTAL"})

    divisa = find_index(headers, {"DIV", "DIVISA"})

    pais_indices = [
        i
        for i, header in enumerate(headers)
        if header in {"PAI", "PAIS", "PAIS_ORIGEN", "PAIS_PROCEDENCIA"}
        or header.startswith("PAI")
    ]

    posicion_declarada = None
    for i, header in enumerate(headers):
        if "NCM" in header:
            continue
        if (
            ("POS" in header or "POSICION" in header)
            and ("ARAN" in header or "DECL" in header)
        ):
            posicion_declarada = i
            break

    ncm = find_contains(headers, {"POS_NCM", "NCM"})

    concepto = find_index(headers, {"COD", "CONCEPTO", "ARANCEL_CONCEPTO"})
    if concepto is None:
        concepto = find_contains(headers, {"CONCEPTO", "COD"})

    monto = find_index(headers, {"MONTO", "ARANCEL_MONTO"})
    if monto is None:
        monto = find_contains(headers, {"MONTO"})

    has_declared_position = len(headers) >= 17

    fallback = {
        "aduana": 0,
        "destinacion": 1,
        "item": 2,
        "fecha": 3,
        "importador": 4,
        "medio": 5,
        "unidad": 6,
        "cantidad": 7,
        "valor_item": 8,
        "valor_total": 9,
        "divisa": 10,
        "pais_origen": 11,
        "pais_procedencia": 12,
        "posicion_declarada": 13 if has_declared_position else None,
        "ncm": 14 if has_declared_position else 13,
        "concepto": 15 if has_declared_position else 14,
        "monto": 16 if has_declared_position else 15,
    }

    aduana = fallback["aduana"] if aduana is None else aduana
    destinacion = (
        fallback["destinacion"] if destinacion is None else destinacion
    )
    item = fallback["item"] if item is None else item
    fecha = fallback["fecha"] if fecha is None else fecha
    importador = fallback["importador"] if importador is None else importador
    medio = fallback["medio"] if medio is None else medio
    unidad = fallback["unidad"] if unidad is None else unidad
    cantidad = fallback["cantidad"] if cantidad is None else cantidad
    valor_item = fallback["valor_item"] if valor_item is None else valor_item
    valor_total = fallback["valor_total"] if valor_total is None else valor_total
    divisa = fallback["divisa"] if divisa is None else divisa

    if len(pais_indices) >= 2:
        pais_origen, pais_procedencia = pais_indices[0], pais_indices[1]
    else:
        pais_origen = fallback["pais_origen"]
        pais_procedencia = fallback["pais_procedencia"]

    if posicion_declarada is None:
        posicion_declarada = fallback["posicion_declarada"]

    ncm = fallback["ncm"] if ncm is None else ncm
    concepto = fallback["concepto"] if concepto is None else concepto
    monto = fallback["monto"] if monto is None else monto

    required = [
        aduana,
        destinacion,
        item,
        fecha,
        importador,
        medio,
        unidad,
        cantidad,
        valor_item,
        valor_total,
        pais_origen,
        pais_procedencia,
        ncm,
        concepto,
        monto,
    ]

    if max(required) >= len(headers):
        raise RuntimeError(
            "No pude interpretar el encabezado ARCA. "
            f"Headers detectados: {headers}"
        )

    logger.debug("Layout detectado: %s", headers)

    return Layout(
        aduana=aduana,
        destinacion=destinacion,
        item=item,
        fecha=fecha,
        importador=importador,
        medio=medio,
        unidad=unidad,
        cantidad=cantidad,
        valor_item=valor_item,
        valor_destinacion=valor_total,
        divisa=divisa,
        pais_origen=pais_origen,
        pais_procedencia=pais_procedencia,
        posicion_declarada=posicion_declarada,
        ncm=ncm,
        concepto=concepto,
        monto=monto,
        field_count=len(headers),
        header_normalized=headers,
    )


class LinkCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag.lower() != "a":
            return
        for key, value in attrs:
            if key.lower() == "href" and value:
                self.hrefs.append(value)


def discover_period_url(periodo: str, logger: logging.Logger) -> str:
    year = int(periodo[:4])

    for index_url in ARCA_INDEX_URLS:
        try:
            logger.info("Consultando publicación oficial: %s", index_url)
            req = Request(
                index_url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "text/html,application/xhtml+xml,*/*",
                },
            )
            with urlopen(req, timeout=45) as response:
                final_url = response.geturl()
                body = response.read()

            text = body.decode("utf-8", errors="ignore")
            parser = LinkCollector()
            parser.feed(text)

            for href in parser.hrefs:
                absolute = urljoin(final_url, html.unescape(href))
                decoded = unquote(absolute)
                match = re.search(
                    r"(?<!\d)(20\d{2})(0[1-9]|1[0-2])\.zip",
                    decoded,
                )
                if (
                    match
                    and int(match.group(1)) == year
                    and f"{match.group(1)}{match.group(2)}" == periodo
                ):
                    return absolute
        except Exception as exc:
            logger.warning("No pude leer índice oficial: %s", exc)

    return ""


def validate_zip(path: Path, periodo: str) -> tuple[bool, str]:
    if not path.exists() or path.stat().st_size < 100:
        return False, "archivo inexistente o demasiado pequeño"
    if not zipfile.is_zipfile(path):
        return False, "archivo descargado no es ZIP"

    try:
        with zipfile.ZipFile(path, "r") as zf:
            expected = f"impo_{periodo}.lst".lower()
            names = [Path(name).name.lower() for name in zf.namelist()]
            if expected in names:
                return True, "OK"

            candidates = [
                name
                for name in names
                if name.startswith("impo_") and name.endswith(".lst")
            ]
            if len(candidates) == 1:
                return True, "OK"

            return False, f"no encontré {expected}"
    except Exception as exc:
        return False, f"ZIP no legible: {exc}"


def find_impo_member(zf: zipfile.ZipFile, periodo: str) -> str:
    expected = f"impo_{periodo}.lst".lower()
    for name in zf.namelist():
        if Path(name).name.lower() == expected:
            return name

    candidates = [
        name
        for name in zf.namelist()
        if Path(name).name.lower().startswith("impo_")
        and Path(name).name.lower().endswith(".lst")
    ]
    if len(candidates) == 1:
        return candidates[0]

    raise RuntimeError(
        f"No encontré {expected}. Candidatos: {candidates[:10]}"
    )


def candidate_urls(periodo: str, discovered_url: str) -> list[str]:
    urls: list[str] = []
    if discovered_url:
        urls.append(discovered_url)

    for pattern in DOWNLOAD_URL_PATTERNS:
        url = pattern.format(periodo=periodo)
        if url not in urls:
            urls.append(url)

    return urls


def download_period(
    periodo: str,
    zip_dir: Path,
    logger: logging.Logger,
    retries: int = 5,
) -> tuple[Path, str]:
    zip_dir.mkdir(parents=True, exist_ok=True)
    final_path = zip_dir / f"{periodo}.zip"
    partial_path = zip_dir / f"{periodo}.zip.part"

    if final_path.exists():
        ok, reason = validate_zip(final_path, periodo)
        if ok:
            logger.info(
                "Usando ZIP existente: %s (%.1f MB)",
                final_path.name,
                final_path.stat().st_size / 1024**2,
            )
            return final_path, ""
        logger.warning("ZIP existente inválido: %s", reason)
        final_path.unlink(missing_ok=True)

    discovered = discover_period_url(periodo, logger)

    last_error = "sin respuesta válida"

    for url in candidate_urls(periodo, discovered):
        for attempt in range(1, retries + 1):
            partial_path.unlink(missing_ok=True)
            try:
                logger.info(
                    "Descargando %s | intento %d/%d",
                    url,
                    attempt,
                    retries,
                )

                req = Request(
                    url,
                    headers={
                        "User-Agent": USER_AGENT,
                        "Accept": "application/zip,application/octet-stream,*/*",
                    },
                )

                with urlopen(req, timeout=300) as response:
                    final_url = response.geturl()
                    content_type = (
                        response.headers.get("Content-Type") or ""
                    ).lower()

                    if "text/html" in content_type:
                        sample = response.read(2048).decode(
                            "utf-8",
                            errors="ignore",
                        )
                        raise RuntimeError(
                            "ARCA respondió HTML y no un ZIP: "
                            + sample[:120].replace("\n", " ")
                        )

                    downloaded = 0
                    started = time.time()

                    with partial_path.open("wb") as fh:
                        while True:
                            chunk = response.read(DOWNLOAD_CHUNK)
                            if not chunk:
                                break
                            fh.write(chunk)
                            downloaded += len(chunk)

                            if downloaded and downloaded % (128 * 1024**2) < DOWNLOAD_CHUNK:
                                elapsed = max(time.time() - started, 0.1)
                                logger.info(
                                    "Descargados %.0f MB (%.1f MB/s)",
                                    downloaded / 1024**2,
                                    downloaded / 1024**2 / elapsed,
                                )

                ok, reason = validate_zip(partial_path, periodo)
                if not ok:
                    raise RuntimeError(reason)

                partial_path.replace(final_path)
                logger.info(
                    "ZIP listo: %s (%.1f MB)",
                    final_path.name,
                    final_path.stat().st_size / 1024**2,
                )
                return final_path, final_url

            except HTTPError as exc:
                last_error = f"HTTP {exc.code}"
                logger.warning("%s para %s", last_error, url)
                if exc.code in {400, 403, 404}:
                    break
            except (
                URLError,
                TimeoutError,
                ConnectionError,
                OSError,
                RuntimeError,
            ) as exc:
                last_error = str(exc)
                logger.warning("Error de descarga: %s", exc)

            if attempt < retries:
                wait = min(5 * (2 ** (attempt - 1)), 60)
                logger.info("Reintento en %d s", wait)
                time.sleep(wait)

    raise RuntimeError(
        f"No se pudo descargar ARCA {periodo}: {last_error}"
    )


def flush_rows(
    writer: pq.ParquetWriter,
    rows: list[dict],
) -> int:
    if not rows:
        return 0

    table = pa.Table.from_pylist(rows, schema=RAW_SCHEMA)
    writer.write_table(
        table,
        row_group_size=min(len(rows), BATCH_SIZE),
    )
    count = len(rows)
    rows.clear()
    return count


def parse_month_to_raw(
    *,
    periodo: str,
    zip_path: Path,
    raw_parquet: Path,
    logger: logging.Logger,
) -> tuple[int, str, str]:
    total_lines = 0
    written_rows = 0
    header_line = ""
    member = ""
    rows: list[dict] = []

    raw_parquet.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        member = find_impo_member(zf, periodo)
        logger.info("Leyendo %s", member)

        with zf.open(member, "r") as source, pq.ParquetWriter(
            raw_parquet,
            RAW_SCHEMA,
            compression="zstd",
            use_dictionary=True,
            write_statistics=True,
        ) as writer:
            header_bytes = source.readline()
            if not header_bytes:
                raise RuntimeError("Archivo de importaciones vacío")

            header_line = header_bytes.decode(
                "latin-1",
                errors="replace",
            ).rstrip("\r\n")
            layout = detect_layout(header_line, logger)

            required_max = max(
                layout.aduana,
                layout.destinacion,
                layout.item,
                layout.fecha,
                layout.importador,
                layout.medio,
                layout.unidad,
                layout.cantidad,
                layout.valor_item,
                layout.valor_destinacion,
                layout.pais_origen,
                layout.pais_procedencia,
                layout.ncm,
                layout.concepto,
                layout.monto,
                layout.divisa if layout.divisa is not None else 0,
                (
                    layout.posicion_declarada
                    if layout.posicion_declarada is not None
                    else 0
                ),
            )

            for raw_line in source:
                total_lines += 1

                line = raw_line.decode(
                    "latin-1",
                    errors="replace",
                ).rstrip("\r\n")
                parts = line.split("'")

                if len(parts) <= required_max:
                    continue

                destinacion = value_at(parts, layout.destinacion)
                item = value_at(parts, layout.item)
                ncm = value_at(parts, layout.ncm)

                if not destinacion or not item or not ncm:
                    continue

                cantidad = safe_float(
                    value_at(parts, layout.cantidad)
                )
                valor_item = safe_float(
                    value_at(parts, layout.valor_item)
                )
                valor_destinacion = safe_float(
                    value_at(parts, layout.valor_destinacion)
                )
                unidad = value_at(parts, layout.unidad)
                cantidad_kg = quantity_to_kg(cantidad, unidad)

                usd_por_unidad = None
                if (
                    cantidad is not None
                    and cantidad > 0
                    and valor_item is not None
                ):
                    usd_por_unidad = valor_item / cantidad

                usd_por_kg = None
                if (
                    cantidad_kg is not None
                    and cantidad_kg > 0
                    and valor_item is not None
                ):
                    usd_por_kg = valor_item / cantidad_kg

                rows.append({
                    "periodo": periodo,
                    "anio": int(periodo[:4]),
                    "mes": int(periodo[4:6]),
                    "aduana": value_at(parts, layout.aduana),
                    "destinacion": destinacion,
                    "item": item,
                    "fecha": value_at(parts, layout.fecha),
                    "importador": value_at(parts, layout.importador),
                    "medio_transporte": value_at(parts, layout.medio),
                    "unidad": unidad,
                    "unidad_nombre": UNIT_NAMES.get(
                        unidad,
                        "DESCONOCIDA",
                    ),
                    "cantidad": cantidad,
                    "cantidad_kg": cantidad_kg,
                    "valor_item_usd": valor_item,
                    "valor_destinacion_usd": valor_destinacion,
                    "usd_por_unidad_declarada": usd_por_unidad,
                    "usd_por_kg": usd_por_kg,
                    "divisa": value_at(parts, layout.divisa),
                    "pais_origen": value_at(
                        parts,
                        layout.pais_origen,
                    ),
                    "pais_procedencia": value_at(
                        parts,
                        layout.pais_procedencia,
                    ),
                    "posicion_arancelaria_declarada": value_at(
                        parts,
                        layout.posicion_declarada,
                    ),
                    "ncm": ncm,
                    "concepto": value_at(parts, layout.concepto),
                    "monto_usd": safe_float(
                        value_at(parts, layout.monto)
                    ),
                    "row_hash": sha1_bytes(raw_line),
                })

                if len(rows) >= BATCH_SIZE:
                    written_rows += flush_rows(writer, rows)
                    if written_rows % 1_000_000 < BATCH_SIZE:
                        logger.info(
                            "%s filas válidas escritas",
                            f"{written_rows:,}",
                        )

            written_rows += flush_rows(writer, rows)

    logger.info(
        "Parseo terminado: %s líneas; %s filas válidas",
        f"{total_lines:,}",
        f"{written_rows:,}",
    )

    return written_rows, header_line, member


def build_analytical_parquets(
    *,
    raw_parquet: Path,
    items_path: Path,
    taxes_path: Path,
    logger: logging.Logger,
) -> tuple[int, int]:
    items_path.parent.mkdir(parents=True, exist_ok=True)
    taxes_path.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(database=":memory:")

    try:
        connection.execute("PRAGMA threads=4")
        connection.execute(
            "PRAGMA preserve_insertion_order=false"
        )

        raw = str(raw_parquet).replace("'", "''")
        items = str(items_path).replace("'", "''")
        taxes = str(taxes_path).replace("'", "''")

        connection.execute(f"""
            COPY (
                SELECT
                    periodo,
                    anio,
                    mes,
                    aduana,
                    destinacion,
                    item,
                    fecha,
                    importador,
                    medio_transporte,
                    unidad,
                    unidad_nombre,
                    cantidad,
                    cantidad_kg,
                    valor_item_usd,
                    valor_destinacion_usd,
                    usd_por_unidad_declarada,
                    usd_por_kg,
                    divisa,
                    pais_origen,
                    pais_procedencia,
                    posicion_arancelaria_declarada,
                    ncm
                FROM read_parquet('{raw}')
                QUALIFY row_number() OVER (
                    PARTITION BY
                        periodo,
                        destinacion,
                        item,
                        ncm
                    ORDER BY row_hash
                ) = 1
            )
            TO '{items}' (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                ROW_GROUP_SIZE 100000
            )
        """)

        connection.execute(f"""
            COPY (
                SELECT DISTINCT
                    periodo,
                    destinacion,
                    item,
                    ncm,
                    concepto,
                    monto_usd,
                    row_hash
                FROM read_parquet('{raw}')
                WHERE concepto <> '' OR monto_usd IS NOT NULL
            )
            TO '{taxes}' (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                ROW_GROUP_SIZE 100000
            )
        """)

        items_count = connection.execute(
            f"SELECT count(*) FROM read_parquet('{items}')"
        ).fetchone()[0]

        taxes_count = connection.execute(
            f"SELECT count(*) FROM read_parquet('{taxes}')"
        ).fetchone()[0]

    finally:
        connection.close()

    logger.info(
        "Parquet final: %s ítems únicos; %s filas tributarias",
        f"{items_count:,}",
        f"{taxes_count:,}",
    )

    return int(items_count), int(taxes_count)


def process_period(
    periodo: str,
    out_dir: Path,
    keep_zip: bool,
    verbose: bool,
) -> dict:
    if (
        len(periodo) != 6
        or not periodo.isdigit()
        or not 1 <= int(periodo[4:]) <= 12
    ):
        raise ValueError(
            "periodo debe tener formato YYYYMM"
        )

    logger = setup_logging(verbose)

    out_dir = out_dir.resolve()
    work_dir = out_dir / ".work" / periodo
    zip_dir = work_dir / "zips"
    raw_parquet = work_dir / "raw_lines.parquet"

    items_path = (
        out_dir
        / "data"
        / "items"
        / f"{periodo}.parquet"
    )
    taxes_path = (
        out_dir
        / "data"
        / "taxes"
        / f"{periodo}.parquet"
    )
    metadata_path = (
        out_dir
        / "metadata"
        / "months"
        / f"{periodo}.json"
    )

    metadata_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    started = datetime.now(timezone.utc)

    zip_path, source_url = download_period(
        periodo,
        zip_dir,
        logger,
    )

    raw_rows, header_raw, zip_member = parse_month_to_raw(
        periodo=periodo,
        zip_path=zip_path,
        raw_parquet=raw_parquet,
        logger=logger,
    )

    items_count, taxes_count = build_analytical_parquets(
        raw_parquet=raw_parquet,
        items_path=items_path,
        taxes_path=taxes_path,
        logger=logger,
    )

    finished = datetime.now(timezone.utc)

    manifest = {
        "periodo": periodo,
        "year": int(periodo[:4]),
        "month": int(periodo[4:]),
        "status": "OK",
        "source_url": source_url,
        "zip_member": zip_member,
        "header_raw": header_raw,
        "raw_valid_rows": raw_rows,
        "unique_items": items_count,
        "tax_rows": taxes_count,
        "zip_bytes": zip_path.stat().st_size,
        "items_bytes": items_path.stat().st_size,
        "taxes_bytes": taxes_path.stat().st_size,
        "items_sha256": sha256_file(items_path),
        "taxes_sha256": sha256_file(taxes_path),
        "started_at_utc": started.isoformat(),
        "finished_at_utc": finished.isoformat(),
        "duration_seconds": round(
            (finished - started).total_seconds(),
            3,
        ),
        "generator": "arca_full_to_parquet.py",
    }

    metadata_path.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    raw_parquet.unlink(missing_ok=True)

    if not keep_zip:
        shutil.rmtree(zip_dir, ignore_errors=True)

    logger.info("Manifest: %s", metadata_path)

    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ARCA completo -> Parquet mensual"
    )
    parser.add_argument(
        "--periodo",
        required=True,
        help="Mes YYYYMM, ej. 202608",
    )
    parser.add_argument(
        "--out",
        default="publish",
        help="Directorio de salida",
    )
    parser.add_argument(
        "--keep-zip",
        action="store_true",
        help="Conservar ZIP tras convertir",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    manifest = process_period(
        args.periodo,
        Path(args.out),
        args.keep_zip,
        args.verbose,
    )

    print(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

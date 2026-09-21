#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import zipfile
from pathlib import Path

from arca_full_to_parquet import (
    detect_layout,
    download_period,
    find_impo_member,
    normalize_text,
    quantity_to_kg,
    safe_float,
    setup_logging,
    value_at,
)


def norm_ncm(value: str) -> str:
    return re.sub(r"[^0-9]", "", value or "")


def scan_month(periodo: str, ncm_prefix: str, out_path: Path) -> dict:
    logger = setup_logging(False)
    work = out_path.parent / ".work" / periodo
    zip_path, source_url = download_period(periodo, work / "zips", logger)

    matches = []
    seen = set()

    with zipfile.ZipFile(zip_path, "r") as zf:
        member = find_impo_member(zf, periodo)
        with zf.open(member, "r") as source:
            header = source.readline().decode("latin-1", errors="replace").rstrip("\r\n")
            layout = detect_layout(header, logger)
            required_max = max(
                layout.destinacion, layout.item, layout.fecha, layout.importador,
                layout.unidad, layout.cantidad, layout.valor_item,
                layout.pais_origen, layout.pais_procedencia, layout.ncm,
                layout.posicion_declarada or 0,
            )

            for raw_line in source:
                line = raw_line.decode("latin-1", errors="replace").rstrip("\r\n")
                parts = line.split("'")
                if len(parts) <= required_max:
                    continue

                ncm_raw = value_at(parts, layout.ncm)
                ncm_digits = norm_ncm(ncm_raw)
                if not ncm_digits.startswith(ncm_prefix):
                    continue

                destinacion = value_at(parts, layout.destinacion)
                item = value_at(parts, layout.item)
                key = (destinacion, item, ncm_digits)
                if key in seen:
                    continue
                seen.add(key)

                cantidad = safe_float(value_at(parts, layout.cantidad))
                unidad = value_at(parts, layout.unidad)
                matches.append({
                    "periodo": periodo,
                    "fecha": value_at(parts, layout.fecha),
                    "importador": normalize_text(value_at(parts, layout.importador)),
                    "destinacion": destinacion,
                    "item": item,
                    "ncm": ncm_raw,
                    "ncm_digits": ncm_digits,
                    "posicion_arancelaria_declarada": value_at(parts, layout.posicion_declarada),
                    "pais_origen": value_at(parts, layout.pais_origen),
                    "pais_procedencia": value_at(parts, layout.pais_procedencia),
                    "unidad": unidad,
                    "cantidad": cantidad,
                    "cantidad_kg": quantity_to_kg(cantidad, unidad),
                    "valor_item_usd": safe_float(value_at(parts, layout.valor_item)),
                })

    result = {
        "periodo": periodo,
        "ncm_prefix": ncm_prefix,
        "source_url": source_url,
        "zip_member": member,
        "matches": matches,
        "match_count": len(matches),
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
    return result


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--periodo", required=True)
    ap.add_argument("--ncm-prefix", default="290613")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    result = scan_month(args.periodo, args.ncm_prefix, Path(args.out))
    print(json.dumps({"periodo": args.periodo, "matches": result["match_count"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

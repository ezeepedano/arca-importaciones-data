import logging
import zipfile
from pathlib import Path

import duckdb

from arca_full_to_parquet import parse_month_to_raw, build_analytical_parquets


def test_deduplicates_items_but_keeps_taxes(tmp_path: Path):
    periodo = "202608"
    z = tmp_path / f"{periodo}.zip"
    header = (
        "ADU'REG_DESTINACION'NUM_ITEM'FECHA_OFIC'IMPORTADOR'M'UN'"
        "CANT_UNIDAD_MEDIDA'FOB_DOLAR'FOB_TOTAL'DIV'PAI'PAI'"
        "POS_ARANCELARIA_DECLARADA'POS_NCM'COD'MONTO\n"
    )
    rows = [
        "001'26001IC010001A'1'20260801'IMPORTADOR SA'4'01'100'2000'2000'USD'156'032'290613000000'2906.13.00'IVA'420\n",
        "001'26001IC010001A'1'20260801'IMPORTADOR SA'4'01'100'2000'2000'USD'156'032'290613000000'2906.13.00'GAN'120\n",
        "001'26001IC010001A'2'20260801'OTRO SA'4'01'50'700'700'USD'156'032'210690900000'2106.90.90'IVA'147\n",
    ]
    with zipfile.ZipFile(z, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"impo_{periodo}.lst", header + "".join(rows))

    raw = tmp_path / "raw.parquet"
    items = tmp_path / "items.parquet"
    taxes = tmp_path / "taxes.parquet"
    logger = logging.getLogger("test")

    raw_rows, _, _ = parse_month_to_raw(
        periodo=periodo,
        zip_path=z,
        raw_parquet=raw,
        logger=logger,
    )
    assert raw_rows == 3

    item_count, tax_count = build_analytical_parquets(
        raw_parquet=raw,
        items_path=items,
        taxes_path=taxes,
        logger=logger,
    )
    assert item_count == 2
    assert tax_count == 3

    total = duckdb.sql(
        f"select sum(valor_item_usd) from read_parquet('{items}')"
    ).fetchone()[0]
    assert total == 2700

    declared = duckdb.sql(
        f"select posicion_arancelaria_declarada from read_parquet('{items}') "
        "where item='1'"
    ).fetchone()[0]
    assert declared == "290613000000"

    items_only = tmp_path / "items_only.parquet"
    unused_taxes = tmp_path / "unused_taxes.parquet"

    item_count_only, tax_count_only = build_analytical_parquets(
        raw_parquet=raw,
        items_path=items_only,
        taxes_path=unused_taxes,
        logger=logger,
        include_taxes=False,
    )
    assert item_count_only == 2
    assert tax_count_only is None
    assert items_only.exists()
    assert not unused_taxes.exists()

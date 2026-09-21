import logging
import zipfile
from pathlib import Path

import duckdb

from arca_full_to_parquet import (\n    parse_month_to_raw,\n    build_analytical_parquets,\n    parse_aggregate_month,\n    detect_source_kind,\n)


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


def test_historical_aggregate_layout(tmp_path: Path):
    periodo = "201702"
    z = tmp_path / f"{periodo}.zip"
    header = (
        "T'FECHA'ADU'POS_NCM'PAI'M'UN'PESO_NETO_KILOS'"
        "MONTO_FOB_DOLAR'CANT_DECLARACIONES'CANT_UNIDAD_ESTADISTICA'"
        "PRECIO_MAX'PRECIO_MIN'PRECIO_PROMEDIO\n"
    )
    rows = [
        "I'20170201'001'21069090'156'4'01'100'2500'2'100'30'20'25\n",
        "I'20170202'002'29061300'032'1'01'50'1200'1'50'24'24'24\n",
    ]
    with zipfile.ZipFile(z, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("impo_agregado_2017_02.lst", header + "".join(rows))

    assert detect_source_kind(header) == "aggregates"

    out = tmp_path / "aggregates.parquet"
    logger = logging.getLogger("test-aggregate")
    count, parsed_header, member = parse_aggregate_month(
        periodo=periodo,
        zip_path=z,
        aggregate_path=out,
        logger=logger,
    )

    assert count == 2
    assert "PESO_NETO_KILOS" in parsed_header
    assert member == "impo_agregado_2017_02.lst"

    result = duckdb.sql(
        f"""
        select
            sum(peso_neto_kg),
            sum(monto_fob_usd),
            sum(cantidad_declaraciones)
        from read_parquet('{out}')
        """
    ).fetchone()
    assert result == (150.0, 3700.0, 3)

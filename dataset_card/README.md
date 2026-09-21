---
pretty_name: ARCA Importaciones Argentina
language:
- es
configs:
- config_name: items
  data_files:
  - split: train
    path: data/items/*.parquet
- config_name: aggregates
  data_files:
  - split: train
    path: data/aggregates/*.parquet
---

# ARCA Importaciones Argentina

Dataset público derivado de la **Información Agregada de Comercio Exterior** publicada por ARCA Argentina.

## Cobertura

El objetivo histórico abarca todos los meses publicados por ARCA desde **02/2017 hasta 08/2026**.

## Dos granularidades reales de ARCA

ARCA no mantuvo el mismo formato durante todo el período. El pipeline detecta el encabezado de cada ZIP y conserva la semántica correcta:

- `data/items/YYYYMM.parquet`: meses con importaciones detalladas por destinación + ítem + NCM. Incluye importador cuando la fuente lo publica.
- `data/aggregates/YYYYMM.parquet`: meses históricos donde ARCA entrega datos agregados con NCM, país, transporte, unidad, peso neto, FOB, cantidad de declaraciones, cantidad estadística y precios min/max/promedio.
- `metadata/months/YYYYMM.json`: manifiesto mensual con `dataset_kind`, fuente, conteos, tamaños y hashes.

**No se deben concatenar `items` y `aggregates` suponiendo que tienen la misma granularidad.**

## Almacenamiento

La tabla de tributos se omitió deliberadamente del histórico para mantener el dataset liviano. En los archivos detallados, ARCA repite cada ítem por concepto tributario; el pipeline deduplica esas filas antes de publicar `items`.

## Consulta detallada con DuckDB

```sql
SELECT importador, SUM(valor_item_usd) AS fob_usd
FROM read_parquet('hf://datasets/alexbozz1/arca-importaciones-argentina/data/items/*.parquet')
WHERE ncm LIKE '2906.13%'
GROUP BY importador
ORDER BY fob_usd DESC;
```

## Consulta agregada histórica

```sql
SELECT ncm, pais, SUM(monto_fob_usd) AS fob_usd
FROM read_parquet('hf://datasets/alexbozz1/arca-importaciones-argentina/data/aggregates/*.parquet')
GROUP BY ncm, pais
ORDER BY fob_usd DESC;
```

## Fuente

Los archivos mensuales se descargan desde la publicación oficial de ARCA. El pipeline no filtra por producto ni por empresa y conserva todos los registros válidos del formato disponible en cada mes.

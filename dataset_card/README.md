---
pretty_name: ARCA Importaciones Argentina
language:
- es
configs:
- config_name: items
  data_files:
  - split: train
    path: data/items/*.parquet
---

# ARCA Importaciones Argentina

Dataset público derivado de la **Información Agregada de Comercio Exterior** publicada por ARCA Argentina.

## Cobertura

El backfill histórico objetivo abarca todos los meses publicados por ARCA desde **02/2017 hasta 08/2026**.

## Tabla principal

- `items`: un registro único por período + destinación + ítem + NCM. Incluye importador, fecha, aduana, transporte, unidad, cantidad, cantidad convertible a kg, valor del ítem, valor de destinación, país de origen/procedencia, posición declarada cuando está disponible y NCM.
- `metadata/months/YYYYMM.json`: trazabilidad mensual, conteos, hashes, tamaño y fuente.

La tabla de tributos se omitió deliberadamente del histórico público para mantener el dataset liviano y gratuito. El archivo original de ARCA repite un ítem por cada concepto tributario; el pipeline deduplica esas filas antes de publicar `items`.

## Advertencias de agregación

- `valor_item_usd` puede agregarse entre ítems, sujeto a la semántica del archivo fuente.
- `valor_destinacion_usd` puede repetirse en varios ítems de una misma destinación. **No sumar esa columna entre ítems** para calcular comercio total.
- `cantidad_kg` solo se calcula cuando la unidad declarada admite una conversión conservadora a kilogramos.

## Consulta con DuckDB

```sql
SELECT importador, SUM(valor_item_usd) AS fob_usd
FROM read_parquet('hf://datasets/alexbozz1/arca-importaciones-argentina/data/items/*.parquet')
WHERE ncm LIKE '2906.13%'
GROUP BY importador
ORDER BY fob_usd DESC;
```

## Fuente

Los archivos mensuales se descargan desde la publicación oficial de ARCA. El pipeline conserva todos los ítems válidos de importaciones; no filtra por producto ni por empresa.

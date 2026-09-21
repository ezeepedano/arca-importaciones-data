---
pretty_name: ARCA Importaciones Argentina
language:
- es
configs:
- config_name: items
  data_files:
  - split: train
    path: data/items/*.parquet
- config_name: taxes
  data_files:
  - split: train
    path: data/taxes/*.parquet
---

# ARCA Importaciones Argentina

Dataset público derivado de la **Información Agregada de Comercio Exterior** publicada por ARCA Argentina.

## Tablas

- `items`: un registro único por período + destinación + ítem + NCM. Es la tabla principal para analizar importadores, NCM, cantidades, FOB, países y USD/kg.
- `taxes`: conceptos/montos tributarios asociados a cada ítem. Se mantiene separada para evitar duplicar FOB o cantidades cuando un ítem tiene varios conceptos.
- `metadata/months/YYYYMM.json`: trazabilidad mensual, conteos, hashes y fuente.

## Advertencias de agregación

- `valor_item_usd` es el valor del ítem y sí puede agregarse entre ítems, sujeto a la semántica del archivo fuente.
- `valor_destinacion_usd` puede repetirse en varios ítems de una misma destinación. **No sumar esa columna entre ítems** para calcular comercio total.
- `cantidad_kg` solo se calcula cuando la unidad declarada admite una conversión conservadora a kilogramos. No se convierten litros, unidades, packs ni kg bruto a kg neto.

## Consulta con DuckDB

```sql
SELECT importador, SUM(valor_item_usd) AS fob_usd
FROM read_parquet('hf://datasets/USUARIO/arca-importaciones-argentina/data/items/*.parquet')
WHERE ncm LIKE '2906.13%'
GROUP BY importador
ORDER BY fob_usd DESC;
```

## Fuente y alcance

Los archivos se descargan de la publicación oficial ARCA/AFIP. El pipeline conserva todos los ítems válidos del archivo mensual de importaciones y no filtra por producto.

El nombre del proveedor/exportador extranjero no necesariamente forma parte de esta fuente pública. El dataset no infiere proveedores.

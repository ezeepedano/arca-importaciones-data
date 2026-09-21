# ARCA → Parquet → Hugging Face

Pipeline cloud para convertir los ZIP mensuales públicos de ARCA Argentina en un dataset Parquet consultable con DuckDB y desde Hugging Face.

## Qué hace

1. Descubre y descarga el ZIP oficial de un mes (`YYYYMM`).
2. Lee `impo_YYYYMM.lst` directamente dentro del ZIP.
3. Conserva **todas** las operaciones válidas, sin filtrar por productos.
4. Separa `items` de `taxes` para no duplicar importes/cantidades.
5. Genera Parquet ZSTD y metadata con hashes SHA-256.
6. GitHub Actions publica el resultado en Hugging Face mediante OIDC, sin token permanente.

## Primer mes

El workflow viene configurado para probar manualmente `202608` (agosto de 2026).

## Configuración única necesaria

En GitHub, crear la variable de Actions:

- `HF_DATASET_REPO=TU_USUARIO_HF/arca-importaciones-argentina`

En Hugging Face, dentro del dataset público `TU_USUARIO_HF/arca-importaciones-argentina`, agregar un Trusted Publisher:

- Provider: GitHub Actions
- repository: `ezeepedano/arca-importaciones-data`
- branch: `main`
- workflow: `publish-arca-month.yml`

El workflow solicita `id-token: write` y usa `HF_OIDC_RESOURCE=datasets/$HF_DATASET_REPO`.

## Ejecución local opcional

```bash
pip install -r requirements.txt
python arca_full_to_parquet.py --periodo 202608 --out publish
```

## Backfill

Una vez validado el primer mes, ejecutar el workflow por cada período histórico disponible. El proceso es idempotente a nivel de archivo mensual: volver a publicar `YYYYMM` reemplaza ese mes en el dataset.

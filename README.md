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

El primer objetivo es `202608` (agosto de 2026).

No hace falta configurar una variable de GitHub ni guardar un token de Hugging Face. El workflow lee el destino desde `hf_dataset_repo.txt`.

## Configuración única de Hugging Face

Crear un dataset público, por ejemplo:

`TU_USUARIO_HF/arca-importaciones-argentina`

Dentro de ese dataset, agregar un Trusted Publisher:

- Provider: GitHub Actions
- repository: `ezeepedano/arca-importaciones-data`
- branch: `main`
- workflow: `publish-arca-month.yml`

El workflow solicita `id-token: write` y usa un token OIDC de corta duración.

## Lanzar un mes

1. Guardar el repo de Hugging Face en `hf_dataset_repo.txt`.
2. Guardar el período en `.run/period.txt`.

Cambiar `.run/period.txt` dispara automáticamente el workflow.

También puede ejecutarse manualmente desde GitHub Actions con `workflow_dispatch`.

## Ejecución local opcional

```bash
pip install -r requirements.txt
python arca_full_to_parquet.py --periodo 202608 --out publish
```

## Backfill

Una vez validado el primer mes, se pueden ir cambiando períodos en `.run/period.txt` para publicar cada mes histórico. Volver a publicar un período reemplaza ese mes en el dataset.

# run_pipeline.py — ingestion + transformation, en une seule commande

import os
import time
import subprocess

# Credentials BigQuery pour la partie Python (dbt lit son propre profiles.yml)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/joseluistorresm/Desktop/cle_bigquery.json"
)

from load_data import ingest_data


def avec_retry(action, essais=3, delai=5):
    """Lance action(). En cas d'erreur, réessaie quelques fois avant d'abandonner."""
    for tentative in range(1, essais + 1):
        try:
            return action()
        except Exception as e:
            print(f"  ⚠️ Échec (tentative {tentative}/{essais}) : {e}")
            if tentative < essais:
                print(f"  ↻ Nouvel essai dans {delai}s…")
                time.sleep(delai)
    raise RuntimeError(f"Abandon après {essais} tentatives.")


# 1. Ingestion : API World Bank → hash → raw_data
print("=== 1. Ingestion (API → raw) ===")
avec_retry(ingest_data, essais=3, delai=5)

# 2. Transformation : dbt build (modèles + tests)
print("\n=== 2. dbt build (raw → staging → marts) ===")
subprocess.run(
    ["uv", "run", "dbt", "build"],
    cwd="world_bank",
    check=True,
)

print("\n✅ Pipeline terminé : données ingérées, transformées et testées.")

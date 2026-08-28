# code load.data.py

import pandas as pd
import requests
import time

from google.cloud import bigquery
from datetime import datetime, timezone
import hashlib
import json


# Indicateurs World Bank sélectionnés pour le projet
INDICATORS = {
    "NY.GDP.MKTP.CD": "PIB",
    "NY.GDP.PCAP.CD": "PIB_par_habitant",
    "NY.GDP.MKTP.KD.ZG": "Croissance_PIB",
    "SP.POP.TOTL": "Population",
    "SP.DYN.LE00.IN": "Esperance_vie",
    "SL.UEM.TOTL.ZS": "Chomage",
    "FP.CPI.TOTL.ZG": "Inflation",
    "EG.ELC.ACCS.ZS": "Acces_electricite",
    "EN.ATM.CO2E.PC": "CO2_par_habitant",
    "SP.DYN.CBRT.IN": "Taux_natalite",
    "SE.SEC.ENRR": "Scolarisation_secondaire",
    "SE.TER.ENRR": "Scolarisation_superieur",
    "SE.PRM.CMPT.ZS": "Achevement_primaire",
    "SH.XPD.CHEX.GD.ZS": "Depense_de_sante",
    "SE.XPD.TOTL.GD.ZS": "Depense_publique_education",
    "SE.ADT.LITR.ZS": "Alphabetisation_adultes",
}

PROJECT_ID = "data-guest-jose-luis"
DATASET_ID = "world_bank_raw"
TABLE_ID = "raw_data"

FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

def calculer_hash(row):
    """Calcule un hash unique à partir des données d'une ligne."""
    contenu = json.dumps(
        row,
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(contenu.encode("utf-8")).hexdigest()

def recuperer_donnees_world_bank():
    """Récupère les indicateurs sélectionnés depuis l'API World Bank."""

    dfs = []

    for code, name in INDICATORS.items():

        url = f"https://api.worldbank.org/v2/country/all/indicator/{code}"
        params = {
            "format": "json",
            "per_page": 20000,
        }

        print(f"\n🔎 Récupération : {name} ({code})")

        try:
            response = requests.get(
                url,
                params=params,
                timeout=30,
            )
        except requests.RequestException as e:
            print(f"❌ Erreur de requête : {e}")
            continue

        if response.status_code != 200:
            print(f"❌ Erreur HTTP : {response.status_code}")
            continue

        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            print("❌ Réponse non-JSON")
            continue

        # Vérification de la structure de la réponse
        if (
            not isinstance(data, list)
            or len(data) < 2
            or not isinstance(data[0], dict)
            or "total" not in data[0]
            or data[1] is None
        ):
            print("❌ Réponse inattendue")
            continue

        total = data[0]["total"]
        print(f"✅ {total} lignes récupérées")

        df = pd.DataFrame(data[1])

        if df.empty:
            print("⚠️ Aucune donnée")
            continue

        # Récupération du nom du pays
        df["country_name"] = df["country"].apply(
            lambda x: x.get("value") if isinstance(x, dict) else None
        )

        df = df[
            [
                "country_name",
                "countryiso3code",
                "date",
                "value",
            ]
        ]

        df = df.rename(columns={"value": name})

        dfs.append(df)

        # Petite pause entre les appels API
        time.sleep(0.5)

    if not dfs:
        raise ValueError(
            "Aucun indicateur n'a pu être récupéré depuis l'API World Bank."
        )

    # Fusion des différents indicateurs
    df_final = dfs[0]

    for df in dfs[1:]:
        df_final = df_final.merge(
            df,
            on=[
                "country_name",
                "countryiso3code",
                "date",
            ],
            how="outer",
        )

    # Nettoyage final
    df_final = df_final.rename(
        columns={"date": "annee"}
    )

    df_final["annee"] = pd.to_numeric(
        df_final["annee"],
        errors="coerce",
    ).astype("Int64")

    df_final = df_final.sort_values(
        ["country_name", "annee"]
    ).reset_index(drop=True)

    print("\n📊 Données finales :")
    print(f"Nombre de lignes : {len(df_final)}")
    print(f"Nombre de colonnes : {len(df_final.columns)}")
    print(df_final.head())

    return df_final

def ingest_data():
    """Récupère les données World Bank et les charge dans BigQuery."""

    print("\n=== INGESTION WORLD BANK → BIGQUERY ===")

    # 1. Récupération des données depuis l'API
    df = recuperer_donnees_world_bank()

    # 2. Création du client BigQuery
    client = bigquery.Client(project=PROJECT_ID)

    # 3. Création d'une copie pour préparer les données
    df = df.copy()

    # 4. Ajout du hash de chaque ligne
    df["row_hash"] = df.apply(
        lambda row: calculer_hash(row.to_dict()),
        axis=1,
    )

    # 5. Ajout de la date d'insertion
    df["inserted_at"] = datetime.now(timezone.utc)

    # 6. Vérification si la table existe déjà
    try:
        table = client.get_table(FULL_TABLE_ID)
        print(f"✅ Table trouvée : {FULL_TABLE_ID}")

        # Récupération des hash déjà présents
        query = f"""
            SELECT row_hash
            FROM `{FULL_TABLE_ID}`
        """

        existing_hashes = {
            row.row_hash
            for row in client.query(query).result()
        }

        print(f"🔎 Hash déjà présents : {len(existing_hashes)}")

    except Exception:
        print("ℹ️ La table n'existe pas encore, elle sera créée.")
        existing_hashes = set()

    # 7. Suppression des lignes déjà présentes
    df_new = df[
        ~df["row_hash"].isin(existing_hashes)
    ].copy()

    print(f"🆕 Nouvelles lignes : {len(df_new)}")

    if df_new.empty:
        print("✅ Aucune nouvelle donnée à insérer.")
        return

    # 8. Configuration du chargement BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )

    # 9. Conversion des valeurs pandas problématiques
    df_new = df_new.astype(object).where(
        pd.notna(df_new),
        None,
    )

    # 10. Chargement dans BigQuery
    load_job = client.load_table_from_dataframe(
        df_new,
        FULL_TABLE_ID,
        job_config=job_config,
    )

    load_job.result()

    print(
        f"✅ {len(df_new)} nouvelles lignes insérées "
        f"dans {FULL_TABLE_ID}"
    )

if __name__ == "__main__":
    ingest_data()
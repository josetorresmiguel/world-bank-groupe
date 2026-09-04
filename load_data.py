import hashlib
import json
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

# Lit le fichier .env et met son contenu dans les variables d'environnement
load_dotenv()


# Codes des indicateurs à récupérer.
# Le nom en français ne sert qu'à l'affichage dans la console : il n'est
# jamais envoyé à BigQuery. Le nom officiel de l'indicateur arrive déjà
# dans le champ indicator.value renvoyé par l'API.
INDICATORS = {
    "NY.GDP.MKTP.CD": "PIB",
    "NY.GDP.PCAP.CD": "PIB par habitant",
    "NY.GDP.MKTP.KD.ZG": "Croissance du PIB",
    "SP.POP.TOTL": "Population",
    "SP.DYN.LE00.IN": "Espérance de vie",
    "SL.UEM.TOTL.ZS": "Chômage",
    "FP.CPI.TOTL.ZG": "Inflation",
    "EG.ELC.ACCS.ZS": "Accès à l'électricité",
    "EN.GHG.CO2.PC.CE.AR5": "CO2 par habitant",
    "SP.DYN.CBRT.IN": "Taux de natalité",
    "SE.SEC.ENRR": "Scolarisation secondaire",
    "SE.TER.ENRR": "Scolarisation supérieur",
    "SE.PRM.CMPT.ZS": "Achèvement du primaire",
    "SH.XPD.CHEX.GD.ZS": "Dépense de santé",
    "SE.XPD.TOTL.GD.ZS": "Dépense publique d'éducation",
    "SE.ADT.LITR.ZS": "Alphabétisation des adultes",
}

# Ces valeurs viennent du .env, elles ne sont plus écrites en dur
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID")

FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Nombre de hash relus dans BigQuery à chaque exécution.
# Doit rester supérieur au nombre de lignes de la table, sinon les lignes
# les plus anciennes ne sont pas reconnues et sont réinsérées en doublon.
# Aujourd'hui : 16 indicateurs × 17490 lignes = 279 840.
LIMITE_HASH = 300000


def calculer_hash(ligne):
    """
    Calcule un hash unique à partir d'une ligne renvoyée par l'API.

    sort_keys=True trie les clés : sans ça, deux dictionnaires identiques
    mais écrits dans un ordre différent donneraient deux hash différents.
    """
    contenu = json.dumps(
        ligne,
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(contenu.encode("utf-8")).hexdigest()


def recuperer_indicateur(code):
    """
    Récupère toutes les pages d'un indicateur.
    Renvoie une liste de dictionnaires, tels que l'API les envoie.

    L'API répond toujours sous la forme [métadonnées, données] :
      data[0] = {"page": 1, "pages": 2, "per_page": 20000, "total": 17556}
      data[1] = [ligne, ligne, ...]
    """

    url = f"https://api.worldbank.org/v2/country/all/indicator/{code}"

    lignes = []
    page = 1
    total = None

    while True:
        params = {
            "format": "json",
            "per_page": 20000,
            "page": page,
        }

        try:
            response = requests.get(
                url,
                params=params,
                timeout=30,
            )
        except requests.RequestException as e:
            print(f"❌ Erreur de requête (page {page}) : {e}")
            return []

        if response.status_code != 200:
            print(f"❌ Erreur HTTP : {response.status_code}")
            return []

        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            print("❌ Réponse non-JSON")
            return []

        # Vérification de la structure de la réponse
        if (
            not isinstance(data, list)
            or len(data) < 2
            or not isinstance(data[0], dict)
            or "total" not in data[0]
            or data[1] is None
        ):
            print("❌ Réponse inattendue")
            return []

        meta = data[0]

        # Le total ne change pas d'une page à l'autre, on le lit une seule fois
        if total is None:
            total = meta.get("total")

        lignes.extend(data[1])

        # C'est ici qu'on regarde s'il reste des pages à lire
        if page >= meta.get("pages", 1):
            break

        page += 1
        time.sleep(0.5)

    # Contrôle : autant de lignes reçues que de lignes annoncées ?
    if total is not None and len(lignes) != total:
        print(f"⚠️ {len(lignes)} lignes reçues pour {total} annoncées")
    else:
        print(f"✅ {len(lignes)} lignes récupérées")

    return lignes


def recuperer_donnees_world_bank():
    """Boucle sur les indicateurs et rassemble toutes les lignes."""

    lignes = []

    for code, nom in INDICATORS.items():
        print(f"\n🔎 Récupération : {nom} ({code})")
        lignes.extend(recuperer_indicateur(code))

        # Petite pause entre les appels API
        time.sleep(0.5)

    if not lignes:
        raise ValueError(
            "Aucun indicateur n'a pu être récupéré depuis l'API World Bank."
        )

    print(f"\n📊 Total de lignes récupérées : {len(lignes)}")

    return lignes


def ingest_data():
    """Récupère les données World Bank et les charge dans BigQuery."""

    print("\n=== INGESTION WORLD BANK → BIGQUERY ===")

    # 1. Récupération des données depuis l'API
    lignes = recuperer_donnees_world_bank()

    # 2. Création du client BigQuery.
    #    Pas de project= : il est déjà dans la clé de service.
    client = bigquery.Client()

    # 3. Vérification si la table existe déjà.
    #    Seul get_table est dans le try : c'est la seule ligne qui peut
    #    lever NotFound. La requête SQL vient après, une fois qu'on sait
    #    que la table est là.
    table_existe = True

    try:
        client.get_table(FULL_TABLE_ID)
    except NotFound:
        table_existe = False

    if table_existe:
        print(f"✅ Table trouvée : {FULL_TABLE_ID}")

        # On ne relit pas toute la table, seulement les lignes récentes
        query = f"""
            SELECT row_hash
            FROM `{FULL_TABLE_ID}`
            ORDER BY inserted_at DESC
            LIMIT {LIMITE_HASH}
        """

        existing_hashes = {
            row.row_hash
            for row in client.query(query).result()
        }

        print(f"🔎 Hash déjà présents : {len(existing_hashes)}")

    else:
        print("ℹ️ La table n'existe pas encore, elle sera créée.")
        existing_hashes = set()

    # 4. Hash de chaque ligne et suppression des lignes déjà présentes.
    #    isoformat() car json ne sait pas sérialiser un objet datetime.
    inserted_at = datetime.now(timezone.utc).isoformat()

    lignes_nouvelles = []

    for ligne in lignes:
        # Le hash porte sur une seule observation : un pays, une année,
        # un indicateur. Une nouvelle publication ne réinsère que sa ligne.
        row_hash = calculer_hash(ligne)

        if row_hash in existing_hashes:
            continue

        lignes_nouvelles.append(
            {
                **ligne,
                "row_hash": row_hash,
                "inserted_at": inserted_at,
            }
        )

    print(f"🆕 Nouvelles lignes : {len(lignes_nouvelles)}")

    if not lignes_nouvelles:
        print("✅ Aucune nouvelle donnée à insérer.")
        return

    # 5. Configuration du chargement BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )

    # 6. Chargement en JSON, sans passer par pandas
    load_job = client.load_table_from_json(
        lignes_nouvelles,
        FULL_TABLE_ID,
        job_config=job_config,
    )

    load_job.result()

    print(
        f"✅ {len(lignes_nouvelles)} nouvelles lignes insérées "
        f"dans {FULL_TABLE_ID}"
    )


if __name__ == "__main__":
    ingest_data()
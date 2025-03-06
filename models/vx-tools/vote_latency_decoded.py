import pandas as pd
import base64

def model(dbt, session):
    # Charger les données brutes (source définie dans dbt)
    raw_data = dbt.ref("vote_latency")  # Nom de votre table brute

    # Conversion du DataFrame brut en pandas
    df = raw_data.to_pandas()

    # Fonction pour décoder les votes Base64
    def decode_base64(encoded_string):
        try:
            return list(base64.b64decode(encoded_string))
        except Exception as e:
            dbt.log(f"Erreur de décodage pour {encoded_string}: {e}")
            return None

    # Appliquer la transformation
    df["decoded_votes"] = df["votesBase64"].apply(decode_base64)

    # Retourner le DataFrame transformé
    return session.write_dataframe(df, schema="transformed", table_name="decoded_epochs_votes")

"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import pandas as pd

def clean_client(client, df):

    client = pd.concat(
        [
            client,
            df[
                [
                    "client_id",
                    "age",
                    "job",
                    "marital",
                    "education",
                    "credit_default",
                    "mortgage",
                ]
            ],
        ],
        ignore_index=True,
    )

    client["job"] = (
        client["job"]
        .str.replace(".", "", regex=False)
        .str.replace("-", "_", regex=False)
    )

    client["education"] = (
        client["education"].str.replace(".", "_", regex=False).replace("unknown", pd.NA)
    )

    client["credit_default"] = (client["credit_default"] == "yes").astype(int)

    client["mortgage"] = (client["mortgage"] == "yes").astype(int)

    return client


def clean_campaign(campaign, df):

    campaign = pd.concat(
        [
            campaign,
            df[
                [
                    "client_id",
                    "month",
                    "day",
                    "number_contacts",
                    "contact_duration",
                    "previous_campaign_contacts",
                    "previous_outcome",
                    "campaign_outcome",
                ]
            ],
        ],
        ignore_index=True,
    )

    campaign["previous_outcome"] = (campaign["previous_outcome"] == "success").astype(
        int
    )

    campaign["campaign_outcome"] = (campaign["campaign_outcome"] == "yes").astype(int)

    meses = {
        "jan": "01",
        "feb": "02",
        "mar": "03",
        "apr": "04",
        "may": "05",
        "jun": "06",
        "jul": "07",
        "aug": "08",
        "sep": "09",
        "oct": "10",
        "nov": "11",
        "dec": "12",
    }

    campaign["month"] = (
        campaign["month"].str.lower().map(meses).fillna(campaign["month"])
    )

    campaign["day"] = campaign["day"].astype(str).str.zfill(2)

    campaign["last_contact_date"] = "2022-" + campaign["month"] + "-" + campaign["day"]

    return campaign


def clean_economics(economics, df):
    economics = pd.concat(
        [
            economics,
            df[["client_id", "cons_price_idx", "euribor_three_months"]],
        ],
        ignore_index=True,
    )

    return economics


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    import zipfile
    import pandas as pd
    import glob
    import os

    client = pd.DataFrame()
    campaign = pd.DataFrame()
    economics = pd.DataFrame()

    zips = glob.glob("files/input/*")

    df_list = []

    for zip_file in zips:
        with zipfile.ZipFile(zip_file, "r") as z:
            with z.open(f"{z.namelist()[0]}") as f:
                temp_df = pd.read_csv(f)
                df_list.append(temp_df)

    df = pd.concat(df_list, ignore_index=True)

    df["client_id"] = df.index

    df = df.rename(
        columns={"campaign": "number_contacts", "duration": "contact_duration"}
    )

    client = clean_client(client, df)
    campaign = clean_campaign(campaign, df)
    economics = clean_economics(economics, df)

    campaign.drop(columns=["month", "day"], inplace=True)

    if os.path.exists("files/output"):
        pass
    else:
        os.mkdir("files/output")

    client.to_csv("files/output/client.csv", index=False)
    campaign.to_csv("files/output/campaign.csv", index=False)
    economics.to_csv("files/output/economics.csv", index=False)


if __name__ == "__main__":
    clean_campaign_data()
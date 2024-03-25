import io
import os

import pandas as pd
import requests
import dotenv

if dotenv.find_dotenv():
    dotenv.load_dotenv()

DATA_DIR = "./data/"
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/18dMo5d89HkyzFGnsQaCPw843LPUG-czAneBR7rVThHI/export?format=csv"
RADARIO_API = "https://api.radar.io/v1/geocode/forward"
FILE_NAME= "htcss_user_registry.csv"

session = requests.Session()
session.headers.update({'Authorization': f'{os.getenv("RADARIO_API_KEY")}'})


def get_location_information(row: pd.Series):

    city = row['City'] if not pd.isna(row['City']) else ''
    state = row['State, Region or Province'] if not pd.isna(row['State, Region or Province']) else ''
    country = row['Country'] if not pd.isna(row['Country']) else ''
    location = f"{city} {state} {country}"

    response = session.get(RADARIO_API, params={
        'query': location
    })
    data = response.json()

    row['Latitude'] = data['addresses'][0]['latitude']
    row['Longitude'] = data['addresses'][0]['longitude']

    return row


def get_gsheet(url) -> pd.DataFrame:
    content = requests.get(url).content

    df = pd.read_csv(io.StringIO(content.decode('utf-8')))

    return df


def main():

    df = get_gsheet(GOOGLE_SHEET_URL)
    df = df.apply(lambda row: get_location_information(row), axis=1)

    if not htcss_user_registry_check(df):
        raise Exception("Some rows have invalid latitude and longitude")

    df.to_csv(DATA_DIR + FILE_NAME, index=False)


def htcss_user_registry_check(df: pd.DataFrame):
    """Returns True if all the rows have a valid latitude and longitude"""
    return not any(df['Latitude'].isna()) and not any(df['Longitude'].isna())


if __name__ == "__main__":
    main()

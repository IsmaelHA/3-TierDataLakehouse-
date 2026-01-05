def download_economy_csv(year: int) -> str:
    import os
    import urllib.request

    # Guardamos una copia por año (aunque la URL sea la misma) para replicar el patrón geometry
    folder = "/tmp/economy"
    os.makedirs(folder, exist_ok=True)

    csv_path = f"{folder}/ine_30824_{year}.csv"
    if os.path.exists(csv_path):
        print(f"Economy CSV for {year} already downloaded, skipping")
        return csv_path

    url = "https://www.ine.es/jaxiT3/files/t/csv_bd/30824.csv"
    urllib.request.urlretrieve(url, csv_path)

    print(f"Economy CSV downloaded for {year}: {csv_path}")
    return csv_path


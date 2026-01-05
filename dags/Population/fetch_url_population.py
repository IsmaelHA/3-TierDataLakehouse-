def download_population_csv(year: int) -> str:
    import os
    import urllib.request

    folder = "/tmp/population"
    os.makedirs(folder, exist_ok=True)

    csv_path = f"{folder}/ine_33571_{year}.csv"

    # Skip if already downloaded
    if os.path.exists(csv_path):
        print(f"Population CSV for {year} already downloaded, skipping")
        return csv_path

    url = "https://www.ine.es/jaxiT3/files/t/es/csv_bd/33571.csv"

    urllib.request.urlretrieve(url, csv_path)
    print(f"Population CSV downloaded for {year}")

    return csv_path


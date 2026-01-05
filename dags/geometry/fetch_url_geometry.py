def download_secciones_censales(year: int):
    import os
    import urllib.request
    import zipfile
    shp_path = f"/tmp/cartografia/España_Seccionado{year}_ETRS89H30/SECC_CE_{year}0101.shp"
    
    # Skip if already downloaded
    if os.path.exists(shp_path):
        print(f"Geometry for {year} already downloaded, skipping")
        return shp_path
    
    url = f"https://www.ine.es/prodyser/cartografia/seccionado_{year}.zip"
    zip_path = f"/tmp/seccionado_{year}.zip"
    extract_path = "/tmp/cartografia/"
    
    urllib.request.urlretrieve(url, zip_path)
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
    
    os.remove(zip_path)
    print(f"Geometry for {year} downloaded")
    
    return shp_path
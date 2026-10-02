"""Prepares the REAL dataset: UCI "Online Retail" (Chen, Sain & Guo, 2012).

541,909 transaction lines of a UK-based online gift retailer, 01-Dec-2010 .. 09-Dec-2011.
Source: https://archive.ics.uci.edu/dataset/352/online+retail   (licence: CC BY 4.0)

  python prepare_data.py            # uses data/online_retail.csv if present, else downloads + converts
  python prepare_data.py --force    # rebuild from the original Excel file
"""
import io, os, sys, urllib.request, zipfile, csv
import config

URLS = ["https://archive.ics.uci.edu/static/public/352/online+retail.zip",
        "https://raw.githubusercontent.com/aakashsyadav1999/online-retail-dataset/main/online%2Bretail.zip"]
RAW_XLSX = os.path.join(config.DATA_DIR, "raw", "Online Retail.xlsx")

# Small reference (lookup) table - one row per country present in the data.
COUNTRY_REGION = {
 "United Kingdom": ("UK & Ireland", "Europe"), "EIRE": ("UK & Ireland", "Europe"), "Channel Islands": ("UK & Ireland", "Europe"),
 "Germany": ("Western Europe", "Europe"), "France": ("Western Europe", "Europe"), "Netherlands": ("Western Europe", "Europe"),
 "Belgium": ("Western Europe", "Europe"), "Switzerland": ("Western Europe", "Europe"), "Austria": ("Western Europe", "Europe"),
 "Spain": ("Southern Europe", "Europe"), "Portugal": ("Southern Europe", "Europe"), "Italy": ("Southern Europe", "Europe"),
 "Greece": ("Southern Europe", "Europe"), "Cyprus": ("Southern Europe", "Europe"), "Malta": ("Southern Europe", "Europe"),
 "Norway": ("Nordics", "Europe"), "Sweden": ("Nordics", "Europe"), "Denmark": ("Nordics", "Europe"),
 "Finland": ("Nordics", "Europe"), "Iceland": ("Nordics", "Europe"),
 "Poland": ("Eastern Europe", "Europe"), "Lithuania": ("Eastern Europe", "Europe"), "Czech Republic": ("Eastern Europe", "Europe"),
 "European Community": ("Europe (unspecified)", "Europe"),
 "Israel": ("Middle East", "Asia"), "Bahrain": ("Middle East", "Asia"), "Lebanon": ("Middle East", "Asia"),
 "United Arab Emirates": ("Middle East", "Asia"), "Saudi Arabia": ("Middle East", "Asia"),
 "Japan": ("Asia-Pacific", "Asia"), "Hong Kong": ("Asia-Pacific", "Asia"), "Singapore": ("Asia-Pacific", "Asia"),
 "Australia": ("Asia-Pacific", "Oceania"),
 "USA": ("North America", "Americas"), "Canada": ("North America", "Americas"), "Brazil": ("South America", "Americas"),
 "RSA": ("Africa", "Africa"), "Unspecified": ("Unspecified", "Unspecified"),
}


def get_excel():
    if os.path.exists(RAW_XLSX):
        return RAW_XLSX
    os.makedirs(os.path.dirname(RAW_XLSX), exist_ok=True)
    for u in URLS:
        try:
            print("Downloading", u)
            blob = urllib.request.urlopen(u, timeout=120).read()
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                name = [n for n in z.namelist() if n.lower().endswith(".xlsx")][0]
                with open(RAW_XLSX, "wb") as f: f.write(z.read(name))
            return RAW_XLSX
        except Exception as e:
            print("  failed:", e)
    sys.exit("Could not download the dataset. Download 'Online Retail.xlsx' from the UCI link above "
             "and place it at data/raw/Online Retail.xlsx")


def main():
    force = "--force" in sys.argv
    if not os.path.exists(config.RETAIL_CSV) or force:
        import pandas as pd
        x = get_excel()
        print("Reading Excel (takes ~30 s) ...")
        df = pd.read_excel(x, dtype={"InvoiceNo": str, "StockCode": str})
        df["CustomerID"] = df["CustomerID"].astype("Int64")            # 17850.0 -> 17850
        df["Description"] = df["Description"].str.strip()
        os.makedirs(config.DATA_DIR, exist_ok=True)
        df.to_csv(config.RETAIL_CSV, index=False, date_format="%Y-%m-%d %H:%M:%S", quoting=csv.QUOTE_MINIMAL)
        print(f"Wrote {len(df):,} rows -> {config.RETAIL_CSV}")
    with open(config.COUNTRY_CSV, "w", newline="") as f:
        w = csv.writer(f); w.writerow(["country", "region", "continent"])
        for c, (r, k) in sorted(COUNTRY_REGION.items()): w.writerow([c, r, k])
    print("Dataset ready:", config.RETAIL_CSV)


if __name__ == "__main__":
    main()

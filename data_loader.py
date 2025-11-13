import pandas as pd
from fredapi import Fred

class DataLoader:

    def __init__(self, api_key):
        self.fred = Fred(api_key=api_key)

    def _get(self, series_dict, freq="ME", yoy=True):
        """Function to download and clean datasets from FRED and ECB."""
        data = {k: self.fred.get_series(v) for k, v in series_dict.items()}
        df = pd.DataFrame(data).resample(freq).last()
        df = df.ffill()
        if yoy:
            df = df.pct_change(12) * 100  

        df = df.dropna(how="any")
        return df

    def load_cpi(self):
        """Load CPI (YoY %) for all countries."""
        codes = {
            "EuroArea": "CP0000EZ19M086NEST",
            "UK": "GBRCPIALLMINMEI",
            "Switzerland": "CHNCPIALLMINMEI",
            "Sweden": "SWECPIALLMINMEI",
            "Norway": "NORCPIALLMINMEI",
            "Poland": "POLCPIALLMINMEI",
            "Hungary": "HUNCPIALLMINMEI",
            "US": "USACPIALLMINMEI"
        }
        return self._get(codes, "ME", yoy=True)

    def load_fx(self):
        """Load FX pairs – all expressed versus USD (USD-based convention)."""

        import requests, io, zipfile, ssl
        ssl._create_default_https_context = ssl._create_unverified_context

        def safe_get(series_id):
            try:
                return self.fred.get_series(series_id)
            except Exception as e:
                print(f"Skipping {series_id} — {type(e).__name__}: {e}")
                return None

        # --- USD-based pairs from FRED ---
        fred_codes = {
            "EURUSD": "DEXUSEU",   # USD per EUR 
            "USDGBP": "DEXUSUK",   # GBP per USD 
            "USDCHF": "DEXSZUS",   # CHF per USD 
            "USDSEK": "DEXSDUS",   # SEK per USD
            "USDNOK": "DEXNOUS",   # NOK per USD
        }

        fx_data = {}

        for name, code in fred_codes.items():
            try:
                data = safe_get(code)
                if data is not None and not data.empty:
                    fx_data[name] = data
            except Exception as e:
                print(f"Could not load Fred data — {type(e).__name__}: {e}")
                continue

        fx = pd.DataFrame(fx_data).resample("B").last().ffill()

        # --- Convert EURUSD to USD-based (currently USD per EUR) ---
        if "EURUSD" in fx.columns:
            fx["USDEUR"] = 1 / fx["EURUSD"]
            fx = fx.drop(columns=["EURUSD"])

        fx = fx.sort_index().dropna(how="any")

        # --- Add PLN & HUF via ECB data ---
        try:
            import requests, io, zipfile
            url = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"
            r = requests.get(url)
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                with z.open("eurofxref-hist.csv") as f:
                    ecb = pd.read_csv(f)

            ecb["Date"] = pd.to_datetime(ecb["Date"], errors="coerce")
            ecb = (
                ecb.set_index("Date")[["PLN", "HUF"]]
                .rename(columns={"PLN": "EURPLN", "HUF": "EURHUF"})
                .sort_index()
                .ffill()
            )

            # Convert to USD-based using USDEUR
            if "USDEUR" in fx.columns:
                for col in ["EURPLN", "EURHUF"]:
                    if col in ecb.columns:
                        fx[f"USD{col[-3:]}"] = fx["USDEUR"] * ecb[col]

        except Exception as e:
            print(f"Could not load ECB PLN/HUF data — {type(e).__name__}: {e}")

        fx = fx.sort_index().dropna(how='any')

        if "USDEUR" in fx.columns:
            cols = ["USDEUR"] + [c for c in fx.columns if c != "USDEUR"]
            fx = fx[cols]
        
        fx = fx.resample("ME").last()

        return fx

    # --- All data combined ---
    def load_all(self):
        """Load all datasets, align them on a common time range, and clean."""
        data = {
            "cpi": self.load_cpi(),
            "fx": self.load_fx()
        }

        start_filter = "2015-01-01"
        end_filter = "2025-12-31"
        aligned = {k: v.loc[start_filter:end_filter] for k, v in data.items()}

        print(f"Datasets loaded: {list(aligned.keys())}")
        return aligned

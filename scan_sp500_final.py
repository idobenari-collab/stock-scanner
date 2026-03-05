#!/usr/bin/env python3
"""
scan_sp500.py - Weekly S&P 500 Snapshot Scanner
Install once:  pip3 install yfinance pandas lxml

Usage:
  python3 scan_sp500.py                # scan all stocks (~25 min)
  python3 scan_sp500.py --top 10       # test with first 10 stocks
  python3 scan_sp500.py --filter 3     # show only stocks with score >= 3
"""

import argparse, json, csv, sys, time
from datetime import datetime

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    print("\n  [ERROR] Run:  pip3 install yfinance pandas lxml\n")
    sys.exit(1)

CSV_FILE  = "sp500_snapshot.csv"
JSON_FILE = "sp500_snapshot.json"

FIELDNAMES = [
    "ticker", "company", "sector",
    "current_price", "trailing_pe", "forward_pe",
    "distance_from_ath_pct", "total_return_1y_pct", "total_return_5y_pct",
    "revenue_growth_5y_pct", "profit_growth_5y_pct",
    "dividend_yield_pct", "payout_ratio_pct",
    "next_earnings_date", "score", "score_breakdown", "scanned_at",
]

SP500 = [
    ("MMM","3M","Industrials"),("AOS","A.O. Smith","Industrials"),
    ("ABT","Abbott","Health Care"),("ABBV","AbbVie","Health Care"),
    ("ACN","Accenture","Information Technology"),("ADBE","Adobe","Information Technology"),
    ("AMD","AMD","Information Technology"),("AES","AES Corp","Utilities"),
    ("AFL","Aflac","Financials"),("A","Agilent","Health Care"),
    ("APD","Air Products","Materials"),("ABNB","Airbnb","Consumer Discretionary"),
    ("AKAM","Akamai","Information Technology"),("ALB","Albemarle","Materials"),
    ("ARE","Alexandria RE","Real Estate"),("ALGN","Align Technology","Health Care"),
    ("ALLE","Allegion","Industrials"),("LNT","Alliant Energy","Utilities"),
    ("ALL","Allstate","Financials"),("GOOGL","Alphabet A","Communication Services"),
    ("GOOG","Alphabet C","Communication Services"),("MO","Altria","Consumer Staples"),
    ("AMZN","Amazon","Consumer Discretionary"),("AMCR","Amcor","Materials"),
    ("AEE","Ameren","Utilities"),("AAL","American Airlines","Industrials"),
    ("AEP","American Electric Power","Utilities"),("AXP","American Express","Financials"),
    ("AIG","American Intl Group","Financials"),("AMT","American Tower","Real Estate"),
    ("AWK","American Water Works","Utilities"),("AMP","Ameriprise","Financials"),
    ("AME","Ametek","Industrials"),("AMGN","Amgen","Health Care"),
    ("APH","Amphenol","Information Technology"),("ADI","Analog Devices","Information Technology"),
    ("ANSS","Ansys","Information Technology"),("AON","Aon","Financials"),
    ("APA","APA Corp","Energy"),("AAPL","Apple","Information Technology"),
    ("AMAT","Applied Materials","Information Technology"),("APTV","Aptiv","Consumer Discretionary"),
    ("ACGL","Arch Capital","Financials"),("ADM","Archer-Daniels","Consumer Staples"),
    ("ANET","Arista Networks","Information Technology"),("AJG","Arthur J Gallagher","Financials"),
    ("AIZ","Assurant","Financials"),("T","AT&T","Communication Services"),
    ("ATO","Atmos Energy","Utilities"),("ADSK","Autodesk","Information Technology"),
    ("ADP","Automatic Data Processing","Information Technology"),
    ("AZO","AutoZone","Consumer Discretionary"),
    ("AVB","AvalonBay","Real Estate"),("AVY","Avery Dennison","Materials"),
    ("AXON","Axon Enterprise","Industrials"),("BKR","Baker Hughes","Energy"),
    ("BALL","Ball Corp","Materials"),("BAC","Bank of America","Financials"),
    ("BK","Bank of NY Mellon","Financials"),("BBWI","Bath & Body Works","Consumer Discretionary"),
    ("BAX","Baxter Intl","Health Care"),("BDX","Becton Dickinson","Health Care"),
    ("BRK-B","Berkshire Hathaway","Financials"),("BBY","Best Buy","Consumer Discretionary"),
    ("BIO","Bio-Rad","Health Care"),("TECH","Bio-Techne","Health Care"),
    ("BIIB","Biogen","Health Care"),("BLK","BlackRock","Financials"),
    ("BX","Blackstone","Financials"),("BA","Boeing","Industrials"),
    ("BWA","BorgWarner","Consumer Discretionary"),("BSX","Boston Scientific","Health Care"),
    ("BMY","Bristol-Myers Squibb","Health Care"),("AVGO","Broadcom","Information Technology"),
    ("BR","Broadridge","Information Technology"),("BRO","Brown & Brown","Financials"),
    ("BF-B","Brown-Forman","Consumer Staples"),("BLDR","Builders FirstSource","Industrials"),
    ("BG","Bunge","Consumer Staples"),("CDNS","Cadence Design","Information Technology"),
    ("CZR","Caesars Entertainment","Consumer Discretionary"),
    ("CPT","Camden Property","Real Estate"),
    ("CPB","Campbell Soup","Consumer Staples"),("COF","Capital One","Financials"),
    ("CAH","Cardinal Health","Health Care"),("KMX","CarMax","Consumer Discretionary"),
    ("CCL","Carnival","Consumer Discretionary"),("CARR","Carrier Global","Industrials"),
    ("CAT","Caterpillar","Industrials"),("CBOE","Cboe Global Markets","Financials"),
    ("CBRE","CBRE Group","Real Estate"),("CDW","CDW Corp","Information Technology"),
    ("CE","Celanese","Materials"),("COR","Cencora","Health Care"),
    ("CNC","Centene","Health Care"),("CNP","CenterPoint Energy","Utilities"),
    ("CF","CF Industries","Materials"),("CRL","Charles River Labs","Health Care"),
    ("SCHW","Charles Schwab","Financials"),
    ("CHTR","Charter Communications","Communication Services"),
    ("CVX","Chevron","Energy"),("CMG","Chipotle","Consumer Discretionary"),
    ("CB","Chubb","Financials"),("CHD","Church & Dwight","Consumer Staples"),
    ("CI","Cigna","Health Care"),("CINF","Cincinnati Financial","Financials"),
    ("CTAS","Cintas","Industrials"),("CSCO","Cisco","Information Technology"),
    ("C","Citigroup","Financials"),("CFG","Citizens Financial","Financials"),
    ("CLX","Clorox","Consumer Staples"),("CME","CME Group","Financials"),
    ("CMS","CMS Energy","Utilities"),("KO","Coca-Cola","Consumer Staples"),
    ("CTSH","Cognizant","Information Technology"),
    ("CL","Colgate-Palmolive","Consumer Staples"),
    ("CMCSA","Comcast","Communication Services"),
    ("CAG","Conagra Brands","Consumer Staples"),("COP","ConocoPhillips","Energy"),
    ("ED","Consolidated Edison","Utilities"),
    ("STZ","Constellation Brands","Consumer Staples"),
    ("CEG","Constellation Energy","Utilities"),("COO","Cooper Companies","Health Care"),
    ("CPRT","Copart","Industrials"),("GLW","Corning","Information Technology"),
    ("CPAY","Corpay","Financials"),("CTVA","Corteva","Materials"),
    ("CSGP","CoStar Group","Real Estate"),("COST","Costco","Consumer Staples"),
    ("CTRA","Coterra Energy","Energy"),("CRWD","CrowdStrike","Information Technology"),
    ("CCI","Crown Castle","Real Estate"),("CSX","CSX Corp","Industrials"),
    ("CMI","Cummins","Industrials"),("CVS","CVS Health","Health Care"),
    ("DHR","Danaher","Health Care"),("DRI","Darden Restaurants","Consumer Discretionary"),
    ("DVA","DaVita","Health Care"),("DECK","Deckers Outdoor","Consumer Discretionary"),
    ("DE","Deere & Co","Industrials"),("DAL","Delta Air Lines","Industrials"),
    ("DVN","Devon Energy","Energy"),("DXCM","Dexcom","Health Care"),
    ("FANG","Diamondback Energy","Energy"),("DLR","Digital Realty","Real Estate"),
    ("DFS","Discover Financial","Financials"),("DG","Dollar General","Consumer Staples"),
    ("DLTR","Dollar Tree","Consumer Staples"),("D","Dominion Energy","Utilities"),
    ("DPZ","Dominos Pizza","Consumer Discretionary"),("DOV","Dover Corp","Industrials"),
    ("DOW","Dow Inc","Materials"),("DHI","D.R. Horton","Consumer Discretionary"),
    ("DTE","DTE Energy","Utilities"),("DUK","Duke Energy","Utilities"),
    ("DD","DuPont","Materials"),("EMN","Eastman Chemical","Materials"),
    ("ETN","Eaton","Industrials"),("EBAY","eBay","Consumer Discretionary"),
    ("ECL","Ecolab","Materials"),("EIX","Edison Intl","Utilities"),
    ("EW","Edwards Lifesciences","Health Care"),
    ("EA","Electronic Arts","Communication Services"),
    ("ELV","Elevance Health","Health Care"),("EMR","Emerson Electric","Industrials"),
    ("ENPH","Enphase Energy","Information Technology"),("ETR","Entergy","Utilities"),
    ("EOG","EOG Resources","Energy"),("EPAM","EPAM Systems","Information Technology"),
    ("EQT","EQT Corp","Energy"),("EFX","Equifax","Industrials"),
    ("EQIX","Equinix","Real Estate"),("EQR","Equity Residential","Real Estate"),
    ("ESS","Essex Property","Real Estate"),("EL","Estee Lauder","Consumer Staples"),
    ("ETSY","Etsy","Consumer Discretionary"),("EG","Everest Group","Financials"),
    ("EVRG","Evergy","Utilities"),("ES","Eversource Energy","Utilities"),
    ("EXC","Exelon","Utilities"),("EXPE","Expedia","Consumer Discretionary"),
    ("EXPD","Expeditors Intl","Industrials"),("EXR","Extra Space Storage","Real Estate"),
    ("XOM","Exxon Mobil","Energy"),("FFIV","F5 Inc","Information Technology"),
    ("FDS","FactSet","Financials"),("FICO","Fair Isaac","Information Technology"),
    ("FAST","Fastenal","Industrials"),("FRT","Federal Realty","Real Estate"),
    ("FDX","FedEx","Industrials"),("FIS","Fidelity National Info","Information Technology"),
    ("FITB","Fifth Third Bancorp","Financials"),
    ("FSLR","First Solar","Information Technology"),("FE","FirstEnergy","Utilities"),
    ("FI","Fiserv","Financials"),("FMC","FMC Corp","Materials"),
    ("F","Ford Motor","Consumer Discretionary"),("FTNT","Fortinet","Information Technology"),
    ("FTV","Fortive","Industrials"),("FOXA","Fox Corp A","Communication Services"),
    ("FOX","Fox Corp B","Communication Services"),("BEN","Franklin Resources","Financials"),
    ("FCX","Freeport-McMoRan","Materials"),("GRMN","Garmin","Consumer Discretionary"),
    ("IT","Gartner","Information Technology"),("GE","GE Aerospace","Industrials"),
    ("GEHC","GE HealthCare","Health Care"),("GEV","GE Vernova","Industrials"),
    ("GEN","Gen Digital","Information Technology"),("GNRC","Generac","Industrials"),
    ("GD","General Dynamics","Industrials"),("GIS","General Mills","Consumer Staples"),
    ("GM","General Motors","Consumer Discretionary"),
    ("GPC","Genuine Parts","Consumer Discretionary"),
    ("GILD","Gilead Sciences","Health Care"),("GPN","Global Payments","Financials"),
    ("GL","Globe Life","Financials"),("GDDY","GoDaddy","Information Technology"),
    ("GS","Goldman Sachs","Financials"),("HAL","Halliburton","Energy"),
    ("HIG","Hartford Financial","Financials"),("HAS","Hasbro","Consumer Discretionary"),
    ("HCA","HCA Healthcare","Health Care"),("DOC","Healthpeak Properties","Real Estate"),
    ("HSIC","Henry Schein","Health Care"),("HSY","Hershey","Consumer Staples"),
    ("HES","Hess Corp","Energy"),
    ("HPE","Hewlett Packard Enterprise","Information Technology"),
    ("HLT","Hilton Worldwide","Consumer Discretionary"),("HOLX","Hologic","Health Care"),
    ("HD","Home Depot","Consumer Discretionary"),("HON","Honeywell","Industrials"),
    ("HRL","Hormel Foods","Consumer Staples"),("HST","Host Hotels","Real Estate"),
    ("HWM","Howmet Aerospace","Industrials"),("HPQ","HP Inc","Information Technology"),
    ("HUBB","Hubbell","Industrials"),("HUM","Humana","Health Care"),
    ("HBAN","Huntington Bancshares","Financials"),
    ("HII","Huntington Ingalls","Industrials"),
    ("IBM","IBM","Information Technology"),("IEX","IDEX Corp","Industrials"),
    ("IDXX","Idexx Laboratories","Health Care"),("ITW","Illinois Tool Works","Industrials"),
    ("INCY","Incyte","Health Care"),("IR","Ingersoll Rand","Industrials"),
    ("PODD","Insulet","Health Care"),("INTC","Intel","Information Technology"),
    ("ICE","Intercontinental Exchange","Financials"),
    ("IFF","Intl Flavors","Materials"),("IP","International Paper","Materials"),
    ("IPG","Interpublic Group","Communication Services"),
    ("INTU","Intuit","Information Technology"),("ISRG","Intuitive Surgical","Health Care"),
    ("IVZ","Invesco","Financials"),("INVH","Invitation Homes","Real Estate"),
    ("IQV","IQVIA Holdings","Health Care"),("IRM","Iron Mountain","Real Estate"),
    ("JKHY","Jack Henry","Information Technology"),("J","Jacobs Solutions","Industrials"),
    ("JNJ","Johnson & Johnson","Health Care"),("JCI","Johnson Controls","Industrials"),
    ("JPM","JPMorgan Chase","Financials"),("JNPR","Juniper Networks","Information Technology"),
    ("K","Kellanova","Consumer Staples"),("KVUE","Kenvue","Consumer Staples"),
    ("KDP","Keurig Dr Pepper","Consumer Staples"),("KEY","KeyCorp","Financials"),
    ("KEYS","Keysight Technologies","Information Technology"),
    ("KMB","Kimberly-Clark","Consumer Staples"),("KIM","Kimco Realty","Real Estate"),
    ("KMI","Kinder Morgan","Energy"),("KKR","KKR & Co","Financials"),
    ("KLAC","KLA Corp","Information Technology"),("KHC","Kraft Heinz","Consumer Staples"),
    ("KR","Kroger","Consumer Staples"),("LHX","L3Harris Technologies","Industrials"),
    ("LH","Laboratory Corp","Health Care"),("LRCX","Lam Research","Information Technology"),
    ("LW","Lamb Weston","Consumer Staples"),
    ("LVS","Las Vegas Sands","Consumer Discretionary"),
    ("LDOS","Leidos Holdings","Industrials"),("LEN","Lennar","Consumer Discretionary"),
    ("LII","Lennox Intl","Industrials"),("LLY","Eli Lilly","Health Care"),
    ("LIN","Linde","Materials"),("LYV","Live Nation","Communication Services"),
    ("LKQ","LKQ Corp","Consumer Discretionary"),("LMT","Lockheed Martin","Industrials"),
    ("L","Loews Corp","Financials"),("LOW","Lowes","Consumer Discretionary"),
    ("LULU","Lululemon","Consumer Discretionary"),("LYB","LyondellBasell","Materials"),
    ("MTB","M&T Bank","Financials"),("MRO","Marathon Oil","Energy"),
    ("MPC","Marathon Petroleum","Energy"),("MKTX","MarketAxess","Financials"),
    ("MAR","Marriott Intl","Consumer Discretionary"),("MMC","Marsh McLennan","Financials"),
    ("MLM","Martin Marietta","Materials"),("MAS","Masco","Industrials"),
    ("MA","Mastercard","Financials"),("MTCH","Match Group","Communication Services"),
    ("MKC","McCormick","Consumer Staples"),("MCD","McDonalds","Consumer Discretionary"),
    ("MCK","McKesson","Health Care"),("MDT","Medtronic","Health Care"),
    ("MRK","Merck","Health Care"),("META","Meta Platforms","Communication Services"),
    ("MET","MetLife","Financials"),("MTD","Mettler-Toledo","Health Care"),
    ("MGM","MGM Resorts","Consumer Discretionary"),
    ("MCHP","Microchip Technology","Information Technology"),
    ("MU","Micron Technology","Information Technology"),
    ("MSFT","Microsoft","Information Technology"),
    ("MAA","Mid-America Apartment","Real Estate"),("MRNA","Moderna","Health Care"),
    ("MHK","Mohawk Industries","Consumer Discretionary"),
    ("MOH","Molina Healthcare","Health Care"),("TAP","Molson Coors","Consumer Staples"),
    ("MDLZ","Mondelez","Consumer Staples"),
    ("MPWR","Monolithic Power","Information Technology"),
    ("MNST","Monster Beverage","Consumer Staples"),("MCO","Moodys","Financials"),
    ("MS","Morgan Stanley","Financials"),("MOS","Mosaic","Materials"),
    ("MSI","Motorola Solutions","Information Technology"),("MSCI","MSCI Inc","Financials"),
    ("NDAQ","Nasdaq","Financials"),("NTAP","NetApp","Information Technology"),
    ("NFLX","Netflix","Communication Services"),("NEM","Newmont","Materials"),
    ("NEE","NextEra Energy","Utilities"),("NKE","Nike","Consumer Discretionary"),
    ("NI","NiSource","Utilities"),("NSC","Norfolk Southern","Industrials"),
    ("NTRS","Northern Trust","Financials"),("NOC","Northrop Grumman","Industrials"),
    ("NCLH","Norwegian Cruise Line","Consumer Discretionary"),
    ("NRG","NRG Energy","Utilities"),("NUE","Nucor","Materials"),
    ("NVDA","Nvidia","Information Technology"),("NVR","NVR Inc","Consumer Discretionary"),
    ("NXPI","NXP Semiconductors","Information Technology"),
    ("ORLY","OReilly Auto","Consumer Discretionary"),
    ("OXY","Occidental Petroleum","Energy"),
    ("ODFL","Old Dominion Freight","Industrials"),
    ("OMC","Omnicom Group","Communication Services"),
    ("ON","ON Semiconductor","Information Technology"),("OKE","ONEOK","Energy"),
    ("ORCL","Oracle","Information Technology"),("OTIS","Otis Worldwide","Industrials"),
    ("PCAR","Paccar","Industrials"),("PKG","Packaging Corp","Materials"),
    ("PLTR","Palantir","Information Technology"),
    ("PANW","Palo Alto Networks","Information Technology"),
    ("PARA","Paramount Global","Communication Services"),
    ("PH","Parker Hannifin","Industrials"),("PAYX","Paychex","Information Technology"),
    ("PAYC","Paycom Software","Information Technology"),("PYPL","PayPal","Financials"),
    ("PNR","Pentair","Industrials"),("PEP","PepsiCo","Consumer Staples"),
    ("PFE","Pfizer","Health Care"),("PCG","PGE","Utilities"),
    ("PM","Philip Morris","Consumer Staples"),("PSX","Phillips 66","Energy"),
    ("PNW","Pinnacle West","Utilities"),("PNC","PNC Financial","Financials"),
    ("POOL","Pool Corp","Consumer Discretionary"),("PPG","PPG Industries","Materials"),
    ("PPL","PPL Corp","Utilities"),("PFG","Principal Financial","Financials"),
    ("PG","Procter & Gamble","Consumer Staples"),("PGR","Progressive","Financials"),
    ("PLD","Prologis","Real Estate"),("PRU","Prudential Financial","Financials"),
    ("PEG","Public Service Enterprise","Utilities"),("PTC","PTC Inc","Information Technology"),
    ("PSA","Public Storage","Real Estate"),("PHM","PulteGroup","Consumer Discretionary"),
    ("QCOM","Qualcomm","Information Technology"),("PWR","Quanta Services","Industrials"),
    ("DGX","Quest Diagnostics","Health Care"),("RL","Ralph Lauren","Consumer Discretionary"),
    ("RJF","Raymond James","Financials"),("RTX","RTX Corp","Industrials"),
    ("O","Realty Income","Real Estate"),("REG","Regency Centers","Real Estate"),
    ("REGN","Regeneron","Health Care"),("RF","Regions Financial","Financials"),
    ("RSG","Republic Services","Industrials"),("RMD","ResMed","Health Care"),
    ("ROK","Rockwell Automation","Industrials"),("ROL","Rollins","Industrials"),
    ("ROP","Roper Technologies","Industrials"),("ROST","Ross Stores","Consumer Discretionary"),
    ("RCL","Royal Caribbean","Consumer Discretionary"),
    ("SPGI","S&P Global","Financials"),("CRM","Salesforce","Information Technology"),
    ("SBAC","SBA Communications","Real Estate"),("SLB","Schlumberger","Energy"),
    ("STX","Seagate Technology","Information Technology"),("SRE","Sempra","Utilities"),
    ("NOW","ServiceNow","Information Technology"),("SHW","Sherwin-Williams","Materials"),
    ("SPG","Simon Property Group","Real Estate"),
    ("SWKS","Skyworks Solutions","Information Technology"),
    ("SNA","Snap-on","Industrials"),("SO","Southern Co","Utilities"),
    ("LUV","Southwest Airlines","Industrials"),
    ("SWK","Stanley Black & Decker","Industrials"),
    ("SBUX","Starbucks","Consumer Discretionary"),("STT","State Street","Financials"),
    ("STLD","Steel Dynamics","Materials"),("STE","Steris","Health Care"),
    ("SYK","Stryker","Health Care"),("SYF","Synchrony Financial","Financials"),
    ("SNPS","Synopsys","Information Technology"),("SYY","Sysco","Consumer Staples"),
    ("TMUS","T-Mobile US","Communication Services"),
    ("TDY","Teledyne Technologies","Industrials"),("TFX","Teleflex","Health Care"),
    ("TER","Teradyne","Information Technology"),("TSLA","Tesla","Consumer Discretionary"),
    ("TXN","Texas Instruments","Information Technology"),("TXT","Textron","Industrials"),
    ("TMO","Thermo Fisher","Health Care"),("TJX","TJX Companies","Consumer Discretionary"),
    ("TSCO","Tractor Supply","Consumer Discretionary"),
    ("TT","Trane Technologies","Industrials"),("TDG","TransDigm Group","Industrials"),
    ("TRV","Travelers Companies","Financials"),("TRMB","Trimble","Information Technology"),
    ("TFC","Truist Financial","Financials"),("TYL","Tyler Technologies","Information Technology"),
    ("TSN","Tyson Foods","Consumer Staples"),("USB","U.S. Bancorp","Financials"),
    ("UBER","Uber","Industrials"),("UDR","UDR Inc","Real Estate"),
    ("ULTA","Ulta Beauty","Consumer Discretionary"),("UNP","Union Pacific","Industrials"),
    ("UAL","United Airlines","Industrials"),("UPS","United Parcel Service","Industrials"),
    ("URI","United Rentals","Industrials"),("UNH","UnitedHealth Group","Health Care"),
    ("UHS","Universal Health Services","Health Care"),("VLO","Valero Energy","Energy"),
    ("VTR","Ventas","Real Estate"),("VRSN","Verisign","Information Technology"),
    ("VRSK","Verisk Analytics","Industrials"),("VZ","Verizon","Communication Services"),
    ("VRTX","Vertex Pharmaceuticals","Health Care"),("VTRS","Viatris","Health Care"),
    ("VICI","VICI Properties","Real Estate"),("V","Visa","Financials"),
    ("VST","Vistra","Utilities"),("VMC","Vulcan Materials","Materials"),
    ("WRB","W.R. Berkley","Financials"),("GWW","W.W. Grainger","Industrials"),
    ("WAB","Wabtec","Industrials"),("WBA","Walgreens Boots","Consumer Staples"),
    ("WMT","Walmart","Consumer Staples"),("DIS","Walt Disney","Communication Services"),
    ("WBD","Warner Bros Discovery","Communication Services"),
    ("WM","Waste Management","Industrials"),("WAT","Waters Corp","Health Care"),
    ("WEC","WEC Energy","Utilities"),("WFC","Wells Fargo","Financials"),
    ("WELL","Welltower","Real Estate"),("WST","West Pharmaceutical","Health Care"),
    ("WDC","Western Digital","Information Technology"),("WY","Weyerhaeuser","Real Estate"),
    ("WMB","Williams Companies","Energy"),("WTW","Willis Towers Watson","Financials"),
    ("WYNN","Wynn Resorts","Consumer Discretionary"),("XEL","Xcel Energy","Utilities"),
    ("XYL","Xylem","Industrials"),("YUM","Yum Brands","Consumer Discretionary"),
    ("ZBRA","Zebra Technologies","Information Technology"),
    ("ZBH","Zimmer Biomet","Health Care"),("ZTS","Zoetis","Health Care"),
]


def get_tickers(limit=None):
    seen = set()
    result = []
    for sym, name, sector in SP500:
        if sym not in seen:
            seen.add(sym)
            result.append({"ticker": sym, "company": name, "sector": sector})
    if limit:
        result = result[:limit]
    print(f"  Loaded {len(result)} tickers.\n")
    return result


def safe(v, d=2):
    try:
        return round(float(v), d)
    except Exception:
        return None


def fetch_one(sym):
    t = yf.Ticker(sym)
    info = t.info

    price = info.get("currentPrice") or info.get("regularMarketPrice")
    hi52 = info.get("fiftyTwoWeekHigh")
    dist = safe(((price - hi52) / hi52) * 100) if price and hi52 else None

    trailing_pe = safe(info.get("trailingPE"))
    forward_pe = safe(info.get("forwardPE"))

    def hist_ret(period):
        try:
            h = t.history(period=period)
            if h.empty:
                return None
            p0 = h["Close"].iloc[0]
            p1 = h["Close"].iloc[-1]
            divs = h["Dividends"].sum() if "Dividends" in h.columns else 0
            return safe(((p1 - p0 + divs) / p0) * 100)
        except Exception:
            return None

    def fin_cagr(label):
        try:
            fin = t.financials
            if fin is None or fin.empty or label not in fin.index:
                return None
            s = fin.loc[label].dropna()
            if len(s) < 2 or float(s.iloc[-1]) <= 0:
                return None
            y = len(s) - 1
            return safe(((float(s.iloc[0]) / float(s.iloc[-1])) ** (1 / y) - 1) * 100)
        except Exception:
            return None

    # Dividend yield: calculated from dividendRate (annual $ per share) / price
    # This avoids Yahoo's inconsistent dividendYield field entirely
    dy = None
    try:
        annual_div = info.get("dividendRate")
        if annual_div and price and float(price) > 0:
            dy = safe((float(annual_div) / float(price)) * 100)
    except Exception:
        pass

    # Payout ratio: Yahoo returns as decimal (0.45 = 45%)
    pr = None
    try:
        raw_pr = info.get("payoutRatio")
        if raw_pr is not None:
            f = float(raw_pr)
            if 0 < f <= 1:
                pr = safe(f * 100)
            elif f > 1:
                pr = safe(f)
    except Exception:
        pass

    ned = None
    try:
        cal = t.calendar
        if isinstance(cal, dict):
            ed = cal.get("Earnings Date")
            if ed:
                if hasattr(ed, "__iter__") and not isinstance(ed, str):
                    ned = str(list(ed)[0])[:10]
                else:
                    ned = str(ed)[:10]
        elif hasattr(cal, "columns") and "Earnings Date" in cal.columns:
            ned = str(cal["Earnings Date"].iloc[0])[:10]
    except Exception:
        pass

    return {
        "price": price,
        "trailing_pe": trailing_pe,
        "forward_pe": forward_pe,
        "dist": dist,
        "ret1y": hist_ret("1y"),
        "ret5y": hist_ret("5y"),
        "rev5y": fin_cagr("Total Revenue"),
        "ni5y": fin_cagr("Net Income"),
        "dy": dy,
        "pr": pr,
        "ned": ned,
    }


def compute_score(d):
    score = 0
    bd = []
    for val, pos, neg, fn in [
        (d["rev5y"], "Revenue growth 5Y > 0%",    "Revenue growth 5Y <= 0%",   lambda v: v > 0),
        (d["ni5y"],  "Profit growth 5Y > 0%",     "Profit growth 5Y <= 0%",    lambda v: v > 0),
        (d["ret5y"], "Total return 5Y > 0%",      "Total return 5Y <= 0%",     lambda v: v > 0),
        (d["dist"],  "Distance from ATH <= -20%", "Distance from ATH > -20%",  lambda v: v <= -20),
    ]:
        if val is None:
            bd.append(" -  N/A")
        elif fn(val):
            score += 1
            bd.append(f"+1  {pos}  ({val:+.1f}%)")
        else:
            bd.append(f" 0  {neg}  ({val:+.1f}%)")

    dy = d["dy"]
    if dy is None:
        bd.append(" -  Dividend N/A")
    elif dy >= 2:
        score += 1
        bd.append(f"+1  Dividend >= 2%  ({dy:.2f}%)")
    else:
        bd.append(f" 0  Dividend < 2%  ({dy:.2f}%)")

    return score, bd


def save(records):
    import shutil, os
    if os.path.exists(JSON_FILE):
        shutil.copy(JSON_FILE, "sp500_snapshot_prev.json")
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        w.writeheader()
        w.writerows(records)
    print(f"  Saved {len(records)} records -> {CSV_FILE}  &  {JSON_FILE}")


def print_table(records, min_score=0):
    rows = sorted(
        [r for r in records if r.get("score", 0) >= min_score],
        key=lambda r: -r.get("score", 0)
    )
    print()
    print(f"  {'#':<4} {'TICKER':<8} {'COMPANY':<26} {'SCR':>4} "
          f"{'PRICE':>8} {'T.PE':>7} {'F.PE':>7} "
          f"{'DIST%':>7} {'1Y%':>7} {'DIV%':>6}")
    print("  " + "-" * 98)

    def f(v, fmt=".1f"):
        return "N/A" if v is None else f"{v:{fmt}}"

    for i, r in enumerate(rows, 1):
        print(f"  {i:<4} {r['ticker']:<8} {r['company'][:25]:<26} {r.get('score', 0):>4} "
              f"  {f(r.get('current_price'), '.2f'):>7} "
              f"  {f(r.get('trailing_pe')):>6} "
              f"  {f(r.get('forward_pe')):>6} "
              f"  {f(r.get('distance_from_ath_pct')):>6} "
              f"  {f(r.get('total_return_1y_pct')):>6} "
              f"  {f(r.get('dividend_yield_pct')):>5}")

    print(f"\n  {len(rows)} stocks shown (score >= {min_score})\n")


def main():
    p = argparse.ArgumentParser(description="S&P 500 Weekly Scanner")
    p.add_argument("--top",    type=int, default=None, help="Scan only first N stocks")
    p.add_argument("--filter", type=int, default=0,    help="Show only stocks with score >= N")
    args = p.parse_args()

    tickers = get_tickers(limit=args.top)
    total = len(tickers)
    records = []
    errors = []
    start = datetime.now()

    print(f"  Scanning {total} stocks. Estimated time: ~{max(1, total * 2 // 60)} min.\n")

    for i, entry in enumerate(tickers, 1):
        sym = entry["ticker"]
        try:
            d = fetch_one(sym)
            score, bd = compute_score(d)
            rec = {
                "ticker":                sym,
                "company":               entry["company"],
                "sector":                entry["sector"],
                "current_price":         d["price"],
                "trailing_pe":           d["trailing_pe"],
                "forward_pe":            d["forward_pe"],
                "distance_from_ath_pct": d["dist"],
                "total_return_1y_pct":   d["ret1y"],
                "total_return_5y_pct":   d["ret5y"],
                "revenue_growth_5y_pct": d["rev5y"],
                "profit_growth_5y_pct":  d["ni5y"],
                "dividend_yield_pct":    d["dy"],
                "payout_ratio_pct":      d["pr"],
                "next_earnings_date":    d["ned"],
                "score":                 score,
                "score_breakdown":       " | ".join(bd),
                "scanned_at":            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            records.append(rec)
            print(f"  [{i:>3}/{total}] {sym:<8} OK  score={score}/5")
        except Exception as e:
            errors.append(sym)
            print(f"  [{i:>3}/{total}] {sym:<8} ERROR: {e}")

        if i % 50 == 0:
            save(records)
            print(f"  -- checkpoint saved ({i}/{total}) --\n")

        time.sleep(1.5)

    save(records)
    elapsed = (datetime.now() - start).seconds // 60
    print(f"\n  Done in ~{elapsed} min.")
    print(f"  Errors ({len(errors)}): {', '.join(errors) if errors else 'none'}")
    print_table(records, min_score=args.filter)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n  Stopped. Partial results may have been saved.\n")
        sys.exit(0)

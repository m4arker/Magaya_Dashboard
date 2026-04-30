from typing import List
from fastapi import FastAPI
from pydantic import BaseModel
import sqlite3
from pathlib import Path
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from typing import Optional
from fastapi import Query

BASE_DIR = Path(__file__).resolve().parent
DB_FILE = BASE_DIR / "magaya_analytics.db"

app = FastAPI(title="Magaya Analytics API")

app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")

@app.get("/", response_class=HTMLResponse)
def root():
    index_path = BASE_DIR / "static" / "index.html"
    with open(index_path, "r", encoding="utf-8") as f:
        return f.read()

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

#-------------- Data models (for API responses)--------------------

class ShipmentSummaryByConsignee(BaseModel):
    consignee: str
    shipment_count: int
    total_weight: float
    total_pieces: int


class ShipmentVolumeByMonth(BaseModel):
    year: int
    month: int
    shipment_count: int
    total_weight: float
    total_pieces: int

class AirlineWeightSummary(BaseModel):
    carrier: str
    total_weight: float
    shipment_count: int

class StatusSummary(BaseModel):
    status: str
    shipment_count: int
    percentage: float

class AirlineWeightOverTime(BaseModel):
    year: int
    month: int
    carrier: str
    total_weight: float

class ShipmentSearchResult(BaseModel):
    status: Optional[str]
    number: Optional[str]
    date: Optional[str]
    consignee: Optional[str]
    carrier: Optional[str]
    weight: Optional[float]
    pieces: Optional[float]


#----------------- API endpoints-------------------------------------

@app.get("/api/summary/by-consignee", response_model=List[ShipmentSummaryByConsignee])
def summary_by_consignee(
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
):
    """
    Returns shipment count, total weight, and total pieces by consignee.
    """

    conn = get_db_connection()
    cur = conn.cursor()
    
    where_clauses = []
    params = []

    if start_date:
        where_clauses.append("date(date) >= date(?)")
        params.append(start_date)
    if end_date:
        where_clauses.append("date(date) <= date(?)")
        params.append(end_date)

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    sql = f"""
        SELECT
            consignee,
            COUNT(*) AS shipment_count,
            SUM(weight) AS total_weight,
            SUM(pieces) AS total_pieces
        FROM shipments
        {where_sql}
        GROUP BY consignee
        ORDER BY shipment_count DESC
        LIMIT 20
        """
    
    
    
    
    cur.execute(sql, params)
    
    rows = cur.fetchall()
    conn.close()

    return [
        ShipmentSummaryByConsignee(
            consignee=row["consignee"],
            shipment_count=row["shipment_count"] or 0,
            total_weight=row["total_weight"] or 0.0,
            total_pieces=row["total_pieces"] or 0,

        )
        for row in rows
    ]

@app.get("/api/summary/volume-by-month", response_model=List[ShipmentVolumeByMonth])
def volume_by_month(
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
):
    """
    Returns shipment count, total wieght, and total pieces by year/month.
    """

    conn = get_db_connection()
    cur = conn.cursor()
    
    where_clauses = []
    params = []

    if start_date:
        where_clauses.append("date(date) >= date(?)")
        params.append(start_date)
    if end_date:
        where_clauses.append("date(date) <= date(?)")
        params.append(end_date)

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    sql = f"""
        SELECT
            date_year AS year,
            date_month AS month,
            COUNT(*) AS shipment_count,
            SUM(weight) AS total_weight,
            SUM(pieces) AS total_pieces
        FROM shipments
        GROUP BY date_year, date_month
        ORDER BY date_year, date_month
        """
    
    
    
    cur.execute(sql, params)
       
    rows = cur.fetchall()
    conn.close()

    return [
        ShipmentVolumeByMonth(
            year=row["year"],
            month=row["month"],
            shipment_count=row["shipment_count"] or 0,
            total_weight=row["total_weight"] or 0.0,
            total_pieces=row["total_pieces"] or 0, 
        )
        for row in rows
    ]

@app.get("/api/airlines/weight-summary", response_model=list[AirlineWeightSummary])
def airlines_weight_summary():
    """
    Total chargeable weight and shipment count by carrier.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            carrier AS carrier,
            SUM(weight) AS total_weight,
            COUNT(*) AS shipment_count
        FROM shipments
        WHERE carrier IS NOT NULL 
            AND carrier <> ''
            AND carrier NOT IN ('JDL Airbus 700', 'Air JDL')
        GROUP BY carrier
        ORDER BY total_weight DESC
        """
    )
    rows = cur.fetchall()
    conn.close()

    return [
        AirlineWeightSummary(
        carrier=row["carrier"],
        total_weight=row["total_weight"] or 0.0,
        shipment_count=row["shipment_count"] or 0,
        )
        for row in rows
    ]
@app.get("/api/commercial-airlines/weight-summary", response_model=list[AirlineWeightSummary])
def commercial_airlines_weight_summary():
    """
    Total chargeable weight and shipment count by commerical carrier.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            carrier AS carrier,
            SUM(weight) AS total_weight,
            COUNT(*) AS shipment_count
        FROM shipments
        WHERE carrier IS NOT NULL 
            AND carrier <> ''
            AND carrier NOT IN ('JDL Airbus 700', 'Air JDL')
            AND carrier_group = 'Commercial'
        GROUP BY carrier
        ORDER BY total_weight DESC
        """
    )
    rows = cur.fetchall()
    conn.close()

    return [
        AirlineWeightSummary(
        carrier=row["carrier"],
        total_weight=row["total_weight"] or 0.0,
        shipment_count=row["shipment_count"] or 0,
        )
        for row in rows
    ]
@app.get("/api/conquest-airlines/weight-summary", response_model=list[AirlineWeightSummary])
def conquest_airlines_weight_summary():
    """
    Total chargeable weight and shipment count of conquest.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            carrier AS carrier,
            SUM(weight) AS total_weight,
            COUNT(*) AS shipment_count
        FROM shipments
        WHERE carrier IS NOT NULL 
            AND carrier <> ''
            AND carrier NOT IN ('JDL Airbus 700', 'Air JDL')
            AND carrier_group = 'Conquest'
        GROUP BY carrier
        ORDER BY total_weight DESC
        """
    )
    rows = cur.fetchall()
    conn.close()

    return [
        AirlineWeightSummary(
        carrier=row["carrier"],
        total_weight=row["total_weight"] or 0.0,
        shipment_count=row["shipment_count"] or 0,
        )
        for row in rows
    ]
@app.get("/api/ibc-airlines/weight-summary", response_model=list[AirlineWeightSummary])
def ibc_airlines_weight_summary():
    """
    Total chargeable weight and shipment count of IBC
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            carrier AS carrier,
            SUM(weight) AS total_weight,
            COUNT(*) AS shipment_count
        FROM shipments
        WHERE carrier IS NOT NULL 
            AND carrier <> ''
            AND carrier NOT IN ('JDL Airbus 700', 'Air JDL')
            AND carrier_group = 'IBC'
        GROUP BY carrier
        ORDER BY total_weight DESC
        """
    )
    rows = cur.fetchall()
    conn.close()

    return [
        AirlineWeightSummary(
        carrier=row["carrier"],
        total_weight=row["total_weight"] or 0.0,
        shipment_count=row["shipment_count"] or 0,
        )
        for row in rows
    ]
@app.get("/api/floridaair-airlines/weight-summary", response_model=list[AirlineWeightSummary])
def floridaair_airlines_weight_summary():
    """
    Total chargeable weight and shipment count of Florida Air.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            carrier AS carrier,
            SUM(weight) AS total_weight,
            COUNT(*) AS shipment_count
        FROM shipments
        WHERE carrier IS NOT NULL 
            AND carrier <> ''
            AND carrier NOT IN ('JDL Airbus 700', 'Air JDL')
            AND carrier_group = 'Florida Air'
        GROUP BY carrier
        ORDER BY total_weight DESC
        """
    )
    rows = cur.fetchall()
    conn.close()

    return [
        AirlineWeightSummary(
        carrier=row["carrier"],
        total_weight=row["total_weight"] or 0.0,
        shipment_count=row["shipment_count"] or 0,
        )
        for row in rows
    ]
@app.get("/api/status/summary", response_model=list[StatusSummary])
def status_summary():
    """
    Count of shipments by Status, plus perecentage total.
    """

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            status,
            COUNT(*) AS shipment_count
        FROM shipments
        WHERE status IS NOT NULL 
            AND status <> ''
            AND status NOT IN ('Empty', 'Pending')
        GROUP BY status
        ORDER BY shipment_count DESC
        """
    )

    rows = cur.fetchall()
    conn.close()

    total = sum(r["shipment_count"] for r in rows) or 1

    return [
        StatusSummary(
            status=row["status"],
            shipment_count=row["shipment_count"],
            percentage=(row["shipment_count"] / total) * 100.0,

        )
        for row in rows
    ]

@app.get("/api/airlines/weight-over-time", response_model=list[AirlineWeightOverTime])
def airlines_weight_over_time():
    """
    Chargeable weight overtime, by carrier and year, month.
    """

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            date_year AS year,
            date_month AS month,
            carrier AS carrier,
            SUM(weight) AS total_weight
        FROM shipments
        WHERE carrier IS NOT NULL 
            AND carrier <> ''
            AND carrier NOT IN ('JDL Airbus 700', 'Air JDL')
            AND date_year IS NOT NULL and date_month IS NOT NULL
        GROUP BY date_year, date_month, carrier
        ORDER BY date_year, date_month, carrier 
        """
    )

    rows = cur.fetchall()
    conn.close()

    return [
        AirlineWeightOverTime(
            year=row["year"],
            month=row["month"],
            carrier=row["carrier"],
            total_weight=row["total_weight"] or 0.0
        )
        for row in rows
    ]

@app.get("/api/search/shipments", response_model=list[ShipmentSearchResult])
def search_shipments(
    q: str = Query(..., min_length=1, description="Search text"),
    limit: int = Query(50, ge=1, le=500),
):
    conn = get_db_connection()
    cur = conn.cursor()

    like = f"%{q}%"
    cur.execute(
        """
        SELECT
            status,
            number,
            date,
            consignee,
            carrier,
            weight,
            pieces
        FROM shipments
        WHERE
            (consignee LIKE ? or carrier LIKE ?)
        ORDER BY date DESC
        LIMIT ?    
        """,
        (like, like, limit)
    )
    rows = cur.fetchall()
    conn.close()

    return [
        ShipmentSearchResult(
            status=row["status"],
            number=row["number"],
            date=row["date"],
            consignee=row["consignee"],
            carrier=row["carrier"],
            weight=row["weight"],
            pieces=row["pieces"]
        )
        for row in rows
    ]

#--------------------------------------------------------------------------------------------------------------------------------------------------------



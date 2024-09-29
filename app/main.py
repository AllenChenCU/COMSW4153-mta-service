from typing import Union

import uvicorn
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware

from pull import (
    query_table,
    refresh_data,
)
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_headers=["*"])


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/outages/{station}")
def read_outages(station: Union[str, None] = None):

    timestamp_at_save = refresh_data("outages")

    # query from outages table
    outages_query = f"""
        SELECT * 
        FROM outages
        WHERE station = '{station}' and timestamp_at_save >= '{timestamp_at_save}';
    """
    outages_df = query_table(outages_query)
    outages_json = outages_df.to_json(orient="records", date_format="iso")
    return Response(outages_json, media_type="application/json")


@app.get("/equipments/{station}")
def read_equipments(station: Union[str, None] = None):

    timestamp_at_save = refresh_data("equipments")

    # query from outages table
    equipments_query = f"""
            SELECT * 
            FROM equipments
            WHERE station = '{station}' and timestamp_at_save >= '{timestamp_at_save}';
        """
    equipments_df = query_table(equipments_query)
    equipments_json = equipments_df.to_json(orient="records", date_format="iso")
    return Response(equipments_json, media_type="application/json")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5001)

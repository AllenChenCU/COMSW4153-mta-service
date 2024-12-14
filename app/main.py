from typing import Union
import os
import fnmatch

import uvicorn
from fastapi import FastAPI, Response, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from jose import jwt, JWTError

from pull import (
    query_table,
    refresh_data,
)
app = FastAPI()
app.add_middleware(
    CORSMiddleware, 
    allow_origins=["*"], 
    allow_headers=["*"], 
    allow_methods=["*"],
)

# JWT secret and algorithm
SECRET_KEY = os.getenv("JWT_SECRET")
ALGORITHM = "HS256"


# Middleware for JWT Validation
@app.middleware("http")
async def jwt_validation_middleware(request: Request, call_next):
    # Allow public routes without JWT validation
    public_routes = [
        "/", 
        "/outages/*", 
        "/equipments/*", 
    ]  # Add more public routes if needed
    for public_route in public_routes:
        if fnmatch.fnmatch(request.url.path, public_route):
            return await call_next(request)

    # Extract Authorization header
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")

    # Extract and decode JWT token
    token = auth_header.split("Bearer ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        # Optional: Store user information for downstream use
        request.state.user = payload.get("sub")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid JWT token")

    # Call the next middleware or route
    return await call_next(request)


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


# --------- protected endpoints -------------------
@app.get("/protected-outages/{station}")
def protected_read_outages(station: Union[str, None] = None):

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


@app.get("/protected-equipments/{station}")
def protected_read_equipments(station: Union[str, None] = None):

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
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=5001, 
        #ssl_keyfile="/etc/ssl/private/fastapi-selfsigned.key", 
        #ssl_certfile="/etc/ssl/certs/fastapi-selfsigned.crt", 
    )

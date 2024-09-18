# main.py
from fastapi import FastAPI, Query
from database import create_database, create_tables
from connections import ClickHouseConnection
app = FastAPI()
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASE_NAME = 'test'

def get_client():
    return ClickHouseConnection.get_client()
    

@app.on_event("startup")
async def startup_event():
    create_database()
    create_tables()

@app.get("/status")
async def read_status():
    return {"message": "Tables are checked and created if they did not exist."}


@app.get("/users")
async def get_users(start_letter: str = Query("B", min_length=1, max_length=1), limit: int = Query(100, le=100)):
    client = get_client()
    logger.info("Connected to ClickHouse")
    try:
        client.query(f"USE {DATABASE_NAME}")
        query = f"""
            SELECT name FROM users
            WHERE name LIKE '{start_letter}%'
            LIMIT {limit}
        """
        result = client.query(query)
        return result.result_rows
    except Exception as e:
        logger.error(f"Error while querying data: {e}")
        return {"error": str(e)}
    finally:
        logger.info("Closing ClickHouse connection")
        client.close() 
        logger.info("Connection closed successfully")
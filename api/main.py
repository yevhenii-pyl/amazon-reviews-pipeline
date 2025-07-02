from fastapi import FastAPI, HTTPException

from db.cassandra_connector import get_cassandra_session
from routes import reviews

app = FastAPI()
app.include_router(reviews.router)

@app.on_event("startup")
async def startup():
    try:
        app.state.cassandra = get_cassandra_session()
    except Exception as e:
        print(f"Cassandra connection failed: {e}")
        raise e

@app.get("/health")
async def health_check():
    try:
        # simple query to test connection
        app.state.cassandra.execute("SELECT now() FROM system.local")
        return {"status": "ok", "message": "API and Cassandra connected"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cassandra connection error: {e}")

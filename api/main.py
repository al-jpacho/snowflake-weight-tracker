from fastapi import FastAPI
from api.routes import test, weight_logs

app = FastAPI()

app.include_router(test.router, prefix="/api")

app.include_router(weight_logs.router, prefix="/api")

@app.get("/")
def root():
    return {"message": "Weight Tracker API is running."}
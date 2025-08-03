from fastapi import FastAPI
from dotenv import load_dotenv
from api.routes import test, weight_logs

load_dotenv()

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Weight Tracker API is running."}
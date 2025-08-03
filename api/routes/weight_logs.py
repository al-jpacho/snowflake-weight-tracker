from fastapi import APIRouter

router = APIRouter()

@router.get("/")
def get_weight_logs():
    return {"message": "Weight logs endpoint is working."}
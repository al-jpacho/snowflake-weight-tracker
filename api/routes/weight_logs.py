from fastapi import APIRouter
from api.utils.snowflake import query_snowflake

router = APIRouter()

@router.get("/weight-logs")
def get_weight_logs():
    data = query_snowflake("""
                           select * from weight_db.raw.fct_weight_logs
                           """)
    return data
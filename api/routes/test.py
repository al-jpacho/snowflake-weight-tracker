from fastapi import APIRouter
from api.utils.snowflake import query_snowflake

router = APIRouter()

@router.get("/test-connection")
def test_snowflake():
    result = query_snowflake("SELECT CURRENT_TIMESTAMP;")
    return {"snowflake_response": result}

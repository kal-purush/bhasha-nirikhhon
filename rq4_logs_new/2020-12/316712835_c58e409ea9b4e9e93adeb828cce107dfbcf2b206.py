from fastapi import Header, HTTPException

async def get_token_header(x_token: str = Header(...)):
    if x_token != "c29ydGUuYmlvdGVjc2EuY29tL2FkbWluCg==":
        raise HTTPException(status_code=400, detail="X-Token header invalid")

async def get_query_token(token: str):
    if token != "c29ydGUuYmlvdGVjc2EuY29tL2FkbWluCg==":
        error = {"code": "INVALID_TOKEN", "message": "The provided token is not valid." }
        json_compatible_error_data = jsonable_encoder(error)
        raise HTTPException(status_code=403, content=json_compatible_error_data)
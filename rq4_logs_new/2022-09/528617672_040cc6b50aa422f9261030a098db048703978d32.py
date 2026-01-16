from fastapi import Header, HTTPException

async def verify_token(x_token: str = Header()):
	if x_token != "abcBolinhasToken":
		raise HTTPException(status_code=400, detail="X-Token header invalid")

async def verify_key(x_key: str = Header()):
	if x_key != "abcBolinhasKey":
		raise HTTPException(status_code=400, detail="X-Key header invalid")
	return x_key
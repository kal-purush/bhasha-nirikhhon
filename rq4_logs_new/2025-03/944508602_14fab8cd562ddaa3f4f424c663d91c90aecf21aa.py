from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request
from datetime import timedelta
import uvicorn
from fastapi.responses import JSONResponse
from fastapi.responses import RedirectResponse



# Import authentication utilities from auth.py
from routes.auth import (
    create_access_token, get_current_user, verify_password, 
    oauth2_scheme, fake_users_db, ACCESS_TOKEN_EXPIRE_MINUTES
)

# FastAPI App
app = FastAPI()

# Static & Templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Login Route: Generate JWT
@app.post("/token")
def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = fake_users_db.get(form_data.username)
    if not user or not verify_password(form_data.password, user["hashed_password"]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    
    # Ensure expiration is properly set
    expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user["username"]},
        expires_delta=expires
    )
    
    # Store token in an HTTP-only cookie
    # response = JSONResponse(content={"message": "Login successful"})
    response = RedirectResponse(url="/dashboard", status_code=303)
    response.set_cookie(key="access_token", value=access_token, httponly=True, secure=True, samesite="Lax")
    
    return response

# Protected Route: Dashboard
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, current_user: dict = Depends(get_current_user)):
    print("Current User:", current_user)  # Debugging
    return templates.TemplateResponse("dashboard.html", {"request": request, "username": current_user["username"]})

# Home Route
@app.get("/", response_class=HTMLResponse)
def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
    

@app.post("/logout")
def logout():
    response = JSONResponse(content={"message": "Logged out successfully"})
    response.delete_cookie("access_token")
    return response

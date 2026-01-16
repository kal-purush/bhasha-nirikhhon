from fastapi import Depends, HTTPException, Request
from sqlalchemy.orm import Session
from config.database import get_db
from auth.auth_bearer import get_user_id_from_token
from models.models import Users

def get_current_user(request: Request, db: Session = Depends(get_db)):
    token = request.headers.get("Authorization")
    if not token:
        raise HTTPException(status_code=400, detail="Token no proporcionado")
    
    token = token.split(" ")[1]
    user_id = get_user_id_from_token(token)

    if not user_id:
        raise HTTPException(status_code=400, detail="Usuario no autenticado")

    user = db.query(Users).filter(Users.id == user_id).first()

    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    return user_id

def check_permissions_factory(module_name: str):
    def check_permissions(request: Request, user_id: int = Depends(get_current_user)):
        rol = {
            "lectura": False,
            "escritura": False,
            "modificacion": False,
            "eliminacion": False
        }

        # Verificar permisos según el método HTTP y el nombre del módulo
        if request.method == "GET" and not rol["lectura"]:
            raise HTTPException(status_code=403, detail=f"No tiene permisos de lectura en el módulo {module_name}")
        if request.method == "POST" and not rol["escritura"]:
            raise HTTPException(status_code=403, detail=f"No tiene permisos de escritura en el módulo {module_name}")
        if request.method == "PUT" and not rol["modificacion"]:
            raise HTTPException(status_code=403, detail=f"No tiene permisos de modificación en el módulo {module_name}")
        if request.method == "DELETE" and not rol["eliminacion"]:
            raise HTTPException(status_code=403, detail=f"No tiene permisos de eliminación en el módulo {module_name}")

        return user_id

    return check_permissions
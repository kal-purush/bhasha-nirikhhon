from fastapi import APIRouter
from pydantic import BaseModel
router = APIRouter()

class Produto(BaseModel):
    codigo: int = None
    nome: str
    descricao: str
    valor_unitario: int 
    foto: bytes =  None
    
# Criar os endpoints de Funcionario: GET, POST, PUT, DELETE
@router.get("/produto/{id}", tags=["produto"])
def get_produto(id: int):
	return {"msg": "get executado", "id": id}, 200

@router.post("/produto/{id}", tags=["produto"])
def post_produto(id: int, p: Produto):
	return {"msg": "post executado", "id": id, "nome": p.nome, "descricao": p.descricao, "valor": p.valor_unitario}, 200

@router.put("/produto/{id}", tags=["produto"])
def put_produto(id: int, p: Produto):
	return {"msg": "put executado", "id": id, "nome": p.nome, "descricao": p.descricao, "valor": p.valor_unitario}, 201

@router.delete("/produto/{id}", tags=["produto"])
def delete_produto(id: int):
	return {"msg": "delete executado", "id": id}, 201
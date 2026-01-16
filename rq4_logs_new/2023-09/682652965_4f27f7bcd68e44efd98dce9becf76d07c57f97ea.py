"""Файл с тестами эндпоинтов для структурного подразделения."""

from fastapi import status
from httpx import AsyncClient


class TestStructureEndpoint:
    """Класс для тестирования эднпоинтров структурного подразделения."""

    async def test_add_structure_endpoint(self, async_client: AsyncClient):
        """Метод для тестирования эндпоинта добавления структурного подразделения."""
        payload = {
            "name": "structure",
        }

        response = await async_client.post("/structure", json=payload)

        assert response.status_code == status.HTTP_201_CREATED
        assert response.json()["id"] == 1

        response = await async_client.post("/structure", json=payload)

        assert response.status_code == status.HTTP_409_CONFLICT
        assert response.json()["details"] == "Item already exist."

    async def test_get_all_structure_endpoint(self, async_client: AsyncClient):
        """Метод для тестирования эндпоинта получения всех структурных подразделений."""
        response = await async_client.get("/structure")

        assert response.status_code == status.HTTP_200_OK
        assert response.json()["data"][0]["id"] == 1
        assert response.json()["data"][0]["name"] == "structure"

    async def test_update_one_structure_endpoint(self, async_client: AsyncClient):
        """Метод для тестирования эндпоинта обновления структурного подразделения."""
        payload = {
            "name": "updated",
        }
        response = await async_client.put("/structure/1", json=payload)

        assert response.status_code == status.HTTP_200_OK
        assert response.json()["data"][0]["id"] == 1
        assert response.json()["data"][0]["name"] == "updated"

        response = await async_client.put("/structure/2", json=payload)

        assert response.status_code == status.HTTP_409_CONFLICT

    async def test_delete_one_structure_endpoint(self, async_client: AsyncClient):
        """Метод для тестирования эндпоинта обновления структурного подразделения."""
        response = await async_client.delete("/structure/1")

        assert response.status_code == status.HTTP_200_OK
        assert response.json()["id"] == 1

        response = await async_client.delete("/structure/2")

        assert response.status_code == status.HTTP_409_CONFLICT
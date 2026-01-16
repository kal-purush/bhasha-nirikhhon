import pytest
from aiohttp import web
import pytest_asyncio

from writer.routes import setup_routes

import pytest

@pytest_asyncio.fixture
async def app(db_pool):
    app = web.Application()
    app["measurement_kinds"] = ["temperature", "humidity"]
    app["db_pool"] = db_pool
    setup_routes(app)
    yield app


@pytest.mark.asyncio
async def test_store_measurements_handler(aiohttp_client, app, db_pool):
    client = await aiohttp_client(app)
    
    valid_payload = {
        "values": [{"time": 1627848484, "value": 23.5}]
    }
    
    resp = await client.post('/api/v1/measurements/invalid_kind', json=valid_payload)
    assert resp.status == 400

    resp = await client.post('/api/v1/measurements/', json=valid_payload)
    assert resp.status == 404  

    resp = await client.post('/api/v1/measurements/temperature', json={})
    assert resp.status == 400


    try:
        resp = await client.post('/api/v1/measurements/temperature', json=valid_payload)
        assert resp.status == 204

    finally:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM measurements")

@pytest.mark.asyncio
async def test_get_measurements_handler(aiohttp_client, app, db_pool):
    client = await aiohttp_client(app)
    
    # Insert some test data directly into the database
    async with db_pool.acquire() as conn:
        await conn.executemany('''
            INSERT INTO measurements(kind, time, value)
            VALUES($1, to_timestamp($2), $3)
        ''', [
            ("temperature", 1627848484, 23.5),
            ("humidity", 1627848485, 55.2),
        ])

    # Test with valid query parameters for temperature
    resp = await client.get('/api/v1/measurements', params={
        'kind': 'temperature',
        'from_time': '1627848480',
        'to_time': '1627848490'
    })
    assert resp.status == 200
    data = await resp.json()
    assert "temperature" in data
    assert len(data["temperature"]) == 1
    assert data["temperature"][0]["value"] == 23.5

    # Test with valid query parameters for humidity
    resp = await client.get('/api/v1/measurements', params={
        'kind': 'humidity',
        'from_time': '1627848480',
        'to_time': '1627848490'
    })
    assert resp.status == 200
    data = await resp.json()
    assert "humidity" in data
    assert len(data["humidity"]) == 1
    assert data["humidity"][0]["value"] == 55.2

    # Test with multiple kinds using a list of tuples
    resp = await client.get('/api/v1/measurements', params=[
        ('kind', 'temperature'),
        ('kind', 'humidity'),
        ('from_time', '1627848480'),
        ('to_time', '1627848490')
    ])
    assert resp.status == 200
    data = await resp.json()
    assert "temperature" in data
    assert "humidity" in data
    assert len(data["temperature"]) == 1
    assert len(data["humidity"]) == 1
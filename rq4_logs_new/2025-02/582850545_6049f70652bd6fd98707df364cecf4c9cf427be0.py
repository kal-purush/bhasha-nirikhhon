# tests/map_builder.py
from tests.helpers import not_raises
from zombpyg.map.world_config_builder import WorldConfigurationBuilderFactory, SingleMapBuilder, RandomMapBuilder


def test_simple_map_builder():
    map_config = {
        "tag": "SingleMap",
        "parameters": {
            "map_id": "tiny_space_v1",
            "w": 640, 
            "h": 480,
            "initial_zombies": 10,
            "minimum_zombies": 10,
        }
    }
    mb = None
    with not_raises(ValueError):
        wb = WorldConfigurationBuilderFactory.get_world_configuration_builder(map_config)
    assert isinstance(wb, (SingleMapBuilder,))

def test_random_map_builder():
    map_config = {
        "tag": "RandomMap",
        "parameters": [
            {
                "weight": 0.25,
                "map_builder": {
                    "tag": "SingleMap",
                    "parameters": {
                        "map_id": "tiny_space_v1",
                        "w": 640, 
                        "h": 480,
                        "initial_zombies": 10,
                        "minimum_zombies": 10,
                    }
                }
            },
            {
                "weight": 0.75,
                "map_builder": {
                    "tag": "SingleMap",
                    "parameters": {
                        "map_id": "tiny_space_v0",
                        "w": 640, 
                        "h": 480,
                        "initial_zombies": 10,
                        "minimum_zombies": 10,
                    }
                }
            }
        ]
    }
    mb = None
    with not_raises(ValueError):
        wb = WorldConfigurationBuilderFactory.get_world_configuration_builder(map_config)
    assert isinstance(wb, (RandomMapBuilder,))

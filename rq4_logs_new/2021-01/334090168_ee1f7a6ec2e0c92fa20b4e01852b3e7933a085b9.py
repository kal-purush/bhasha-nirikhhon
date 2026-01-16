import sys
import os
import docker
import yaml
from typing import Dict, List

class ContainerConfig:
    def __init__(self, name : str, image : str, expose : Dict, volumes : List, env : List):
        self._name = name
        self._image = image
        self._env = env
        self._volumes = None if volumes is None else self._generate_volumes(volumes)
                
    def _generate_volumes(self, volumes : List):
        if len(volumes) == 0:
            return None
        else:
            volume_list = []
            for elem in volumes:
                volume_list.append({elem.split(":")[0] : {'bind' : elem.split(":")[1], 'mode' : 'rw'}}) 
            if len(volume_list) == 1 :
                return volume_list[0]
            else:
                return volume_list

    def get_name(self) -> str:
        return self._name
    
    def get_image(self) -> str:
        return self._image
    
    def get_volumes(self):
        return self._volumes

    def get_env(self):
        return self._env

class Builder:
    def __init__(self, config_file : str) -> None:
        self._client = docker.from_env()
        self._config_file = config_file
        self._container_config = self._read_yaml_file()
        

    def _read_yaml_file(self):
        config_file = open(self._config_file)
        config = yaml.load(config_file,  Loader=yaml.FullLoader)
        container_config = []
        print(config)
        for container in config["env"]:
            volumes = None if "volumes" not in container else container["volumes"]
            env = None if "env" not in container else container["env"]
            expose = None if "expose" not in container else container["expose"]

            obj = ContainerConfig(name = container["name"], image = container["image"], 
            expose = expose, env = env, volumes = volumes)
            
            container_config.append(obj)
        return container_config
import os
from dataclasses import dataclass

from jinja2 import FileSystemLoader, Environment


@dataclass
class ClusterConfig:
    name: str


@dataclass
class NodeConfig:
    name: str


def render_config(cluster_name: str, node_id: int) -> str:
    template_dir = os.path.join(os.path.dirname(__file__), "templates")
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template("elasticsearch.yml.v2.j2")

    cluster = ClusterConfig(name=cluster_name)
    node = NodeConfig(name=f"{cluster_name}-data-{node_id}")

    return template.render(cluster=cluster, node=node)
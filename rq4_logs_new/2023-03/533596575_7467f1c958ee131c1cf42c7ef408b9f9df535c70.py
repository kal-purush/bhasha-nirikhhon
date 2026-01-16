import fnmatch
import json

import networkx as nx
import pyvis


"""See: https://cerfacs.fr/coop/pycallgraph

Example:
    $ pipx install pycg
    $ pip install pyvis networkx
    $ pycg --package pyrovelocity pyrovelocity/*.py -o tmp.json
    $ python -m callgraph
"""


def to_ntwx_json(data: dict) -> nx.DiGraph:
    nt = nx.DiGraph()

    def _ensure_key(name):
        if name not in nt:
            nt.add_node(name, size=50)

    for node in data:
        _ensure_key(node)
        for child in data[node]:
            _ensure_key(child)
            nt.add_edge(node, child)
    return nt


with open("tmp.json") as fin:
    cgdata = json.load(fin)


ntx: nx.DiGraph = to_ntwx_json(cgdata)
print(ntx.number_of_nodes())


def remove_hyperconnect(ntx: nx.DiGraph, treshold=7):
    to_remove = [
        node for node in ntx.nodes if len(list(ntx.predecessors(node))) >= treshold
    ]
    for node in to_remove:
        ntx.remove_node(node)
    return ntx


# ntx = remove_hyperconnect(ntx)
print(ntx.number_of_nodes())


def remove_by_patterns(ntx: nx.DiGraph, forbidden_names: list = []) -> nx.DiGraph:
    def is_allowed(name):
        for pattern in forbidden_names:
            if fnmatch.filter([name], pattern):
                return False
        return True

    to_remove = []
    for node in ntx.nodes:
        if not is_allowed(node):
            to_remove.append(node)

    for node in to_remove:
        ntx.remove_node(node)

    return ntx


ntx = remove_by_patterns(
    ntx, forbidden_names=["<builtin>*", "numpy*", "scipy*", "pandas*", "tkinter*"]
)
print(ntx.number_of_nodes())

color_filter = {
    "pyrovelocity.api": "red",
    "pyrovelocity.data": "purple",
    "pyrovelocity.plot": "purple",
    "scvi": "green",
    "pyrovelocity.utils": "yellow",
    "_velocity_model": "darkblue",
    "_velocity_module": "blue",
    "_velocity_guide": "cyan",
    "pyrovelocity._velocity.PyroVelocity": "pink",
    "_trainer": "darkred",
    "cytotrace": "gray",
    "pyro.": "orange",
    "default": "black",
}


def ntw_pyvis(ntx: nx.DiGraph, root, size0=5, loosen=2):
    nt = pyvis.network.Network(width="1890px", height="950px", directed=True)
    for node in ntx.nodes:
        mass = ntx.nodes[node]["size"] / (loosen * size0)
        size = size0 * ntx.nodes[node]["size"] ** 0.5
        color = color_filter["default"]
        for key in color_filter:
            if key in node:
                color = color_filter[key]
        kwargs = {
            "label": node,
            "mass": mass,
            "size": size,
            "color": color,
        }
        nt.add_node(
            node,
            **kwargs,
        )

    for link in ntx.edges:
        try:
            depth = nx.shortest_path_length(ntx, source=root, target=link[0])
            width = max(size0, size0 * (12 - 4 * depth))
        except Exception:
            width = 5

        nt.add_edge(link[0], link[1], width=width)

    nt.show_buttons(filter_=["physics"])
    nt.save_graph("nodes.html")


ntw_pyvis(ntx=ntx, root="...pyrovelocity.api.train_model")
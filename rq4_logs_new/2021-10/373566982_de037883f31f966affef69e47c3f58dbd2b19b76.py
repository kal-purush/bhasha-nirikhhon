import datetime
from random import randint


class ParseTopology:

    def __init__(self, topology, version, model_version, oxp_name, oxp_url):
        self.kytos_topology = topology
        self.version = version
        self.model_version = model_version
        self.oxp_name = oxp_name
        self.oxp_url = oxp_url

    def get_kytos_nodes(self):
        return self.kytos_topology["switches"].values()

    def get_kytos_links(self):
        return self.kytos_topology["links"].values()

    @staticmethod
    def get_timestamp():
        """Function to obtain the current time_stamp in a specific format"""
        return datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

    @staticmethod
    def get_port_speed(speed):
        """Function to obtain the speed of a specific port in the network."""
        if speed == 12500000000:
            return "100GE"
        elif speed == 1250000000:
            return "10GE"
        elif speed == 125000000:
            return "1GE"
        else:
            return "Other"

    @staticmethod
    def get_status(status):
        return "up" if status else "down"

    @staticmethod
    def get_state(state):
        return "enabled" if state else "disabled"

    def get_port_urn(self, switch, interface):
        """function to generate the full urn address for a node"""

        if not isinstance(interface, str) and not isinstance(interface, int):
            raise ValueError("Interface is not the proper type")
        if interface == "" or switch == "":
            raise ValueError("Interface and switch CANNOT be empty")
        if isinstance(interface, int) and interface <= 0:
            raise ValueError("Interface cannot be negative")

        try:
            switch_name = self.get_kytos_nodes_names()[switch]
        except KeyError:
            switch_name = switch

        return f"urn:sdx:port:{self.oxp_url}:{switch_name}:{interface}"

    def get_port(self, sdx_node_name, interface):
        """Function to retrieve a network device's port (or interface) """

        sdx_port = dict()
        sdx_port["id"] = self.get_port_urn(sdx_node_name, interface["port_number"])
        sdx_port["name"] = interface["name"]
        sdx_port["node"] = f"urn:sdx:node:{self.oxp_url}:{sdx_node_name}"
        sdx_port["type"] = self.get_port_speed(interface["speed"])
        sdx_port["status"] = self.get_status(interface["active"])
        sdx_port["state"] = self.get_state(interface["enabled"])
        sdx_port["services"] = "l2vpn"
        sdx_port["nni"] = "False"

        if "nni" in interface["metadata"]:
            sdx_port["nni"] = interface["metadata"]["nni"]

        if "mtu" in interface["metadata"]:
            sdx_port["mtu"] = interface["metadata"]["mtu"]
        else:
            sdx_port["mtu"] = 1500

        return sdx_port

    def get_ports(self, sdx_node_name, interfaces):
        """Function that calls the main individual get_port function,
        to get a full list of ports from a node/ interface """
        ports = list()
        for interface in interfaces.values():
            port_no = interface["port_number"]
            if port_no != 4294967294:
                ports.append(self.get_port(sdx_node_name, interface))

        return ports

    def get_kytos_nodes_names(self):
        """Function to retrieve the data_path attribute for every switch form Kytos's topology API"""
        nodes_mappings = {}

        for node in self.get_kytos_nodes():
            if "node_name" in node["metadata"]:
                nodes_mappings[node["id"]] = node["metadata"]["node_name"]
            else:
                nodes_mappings[node["id"]] = node["data_path"]

        return nodes_mappings

    def get_sdx_node(self, kytos_node):
        """function that builds every Node dictionary object with all the necessary
        attributes that make a Node object; the name, id, location and list of
        ports."""
        sdx_node = dict()

        if "node_name" in kytos_node["metadata"]:
            sdx_node["name"] = kytos_node["metadata"]["node_name"]
        else:
            sdx_node["name"] = kytos_node["data_path"]

        sdx_node["id"] = f"urn:sdx:node:{self.oxp_url}:%s" % sdx_node["name"]

        sdx_node["location"] = {"address": "", "latitude": "", "longitude": ""}
        if "address" in kytos_node["metadata"]:
            sdx_node["location"]["address"] = kytos_node["metadata"]["address"]
        if "lat" in kytos_node["metadata"]:
            sdx_node["location"]["latitude"] = kytos_node["metadata"]["lat"]
        if "lng" in kytos_node["metadata"]:
            sdx_node["location"]["longitude"] = kytos_node["metadata"]["lng"]

        sdx_node["ports"] = self.get_ports(sdx_node["name"], kytos_node["interfaces"])

        return sdx_node

    def get_sdx_nodes(self):
        """function that returns a list of SDX Nodes objects for every Kytos node in the topology"""
        sdx_nodes = list()
        for kytos_node in self.get_kytos_nodes():
            if kytos_node["enabled"]:
                sdx_nodes.append(self.get_sdx_node(kytos_node))
        return sdx_nodes

    def get_sdx_port_urn(self, switch, interface):
        """function to generate the full urn address for a node"""

        if not isinstance(interface, str) and not isinstance(interface, int):
            raise ValueError("Interface is not the proper type")
        if interface == "" or switch == "":
            raise ValueError("Interface and switch CANNOT be empty")
        if isinstance(interface, int) and interface <= 0:
            raise ValueError("Interface cannot be negative")

        try:
            switch_name = self.get_kytos_nodes_names()[switch]
        except KeyError:
            switch_name = switch

        return f"urn:sdx:port:{self.oxp_url}:{switch_name}:{interface}"

    def get_sdx_link(self, kytos_link):
        """function that generates a dictionary object for every link in a network,
        and containing all the attributes for each link"""

        sdx_link = dict()
        interface_a = int(kytos_link["endpoint_a"]["id"].split(":")[8])
        switch_a = ":".join(kytos_link["endpoint_a"]["id"].split(":")[0:8])
        interface_b = int(kytos_link["endpoint_b"]["id"].split(":")[8])
        switch_b = ":".join(kytos_link["endpoint_b"]["id"].split(":")[0:8])
        if switch_a == switch_b:
            return sdx_link

        sdx_link["name"] = "%s/%s_%s/%s" % (self.get_kytos_nodes_names()[switch_a], interface_a,
                                        self.get_kytos_nodes_names()[switch_b], interface_b)
        sdx_link["id"] = f"urn:sdx:link:{self.oxp_url}:%s" % sdx_link["name"]
        sdx_link["ports"] = [self.get_sdx_port_urn(switch_a, interface_a),
                         self.get_sdx_port_urn(switch_b, interface_b)]

        sdx_link["type"] = "intra"

        for item in ["bandwidth", "residual_bandwidth", "latency", "packet_loss", "availability"]:
            if item in kytos_link["metadata"]:
                sdx_link[item] = kytos_link["metadata"][item]
            else:
                if item in ["bandwidth"]:
                    sdx_link[item] = self.get_port_speed(kytos_link["endpoint_a"]["speed"])
                elif item in ["residual_bandwidth", "availability"]:
                    sdx_link[item] = 100
                else:
                    sdx_link[item] = 0

        sdx_link["status"] = "up" if kytos_link["endpoint_a"]["active"] else "down"
        sdx_link["state"] = "enabled" if kytos_link["endpoint_a"]["enabled"] else "disabled"

        return sdx_link

    def get_sdx_links(self):
        """function that returns a list of Link objects based on the network's
        devices connections to each other"""

        sdx_links = list()

        for kytos_link in self.get_kytos_links():
            if kytos_link["enabled"]:
                sdx_link = self.get_sdx_link(kytos_link)
                if sdx_link:
                    sdx_links.append(sdx_link)

        return sdx_links

    def create_inter_oxp_link_entries(self):
        """ Create entries for inter-oxp links """
        sdx_links = list()
        for kytos_node in self.get_kytos_nodes():
            for kytos_interface in kytos_node["interfaces"].values():
                if "nni" in kytos_interface["metadata"]:
                    if self.oxp_url not in kytos_interface["metadata"]["nni"]:

                        sdx_link = dict()

                        if "link_name" in kytos_interface["metadata"]:
                            sdx_link["name"] = kytos_interface["metadata"]["link_name"]
                        else:
                            sdx_link["name"] = "NO_NAME_%s" % randint(0, 100000)

                        sdx_link["id"] = f"urn:sdx:link:{self.oxp_url}:{sdx_link['name']}"

                        if "node_name" in kytos_node["metadata"]:
                            kytos_node["name"] = kytos_node["metadata"]["node_name"]
                        else:
                            kytos_node["name"] = kytos_node["data_path"]

                        port_id = self.get_port_urn(kytos_node["id"], kytos_interface["port_number"])

                        sdx_link["ports"] = [port_id, kytos_interface["metadata"]["nni"]]

                        sdx_link["type"] = "inter"
                        sdx_link["bandwidth"] = self.get_port_speed(kytos_interface["speed"])
                        sdx_link["status"] = "up" if kytos_interface["active"] else "down"
                        sdx_link["state"] = "enabled" if kytos_interface["enabled"] else "disabled"

                        sdx_link["availability"] = 100
                        sdx_link["residual_bandwidth"] = 100
                        sdx_link["packet_loss"] = 0
                        sdx_link["latency"] = 0
                        sdx_links.append(sdx_link)
                        del sdx_link
        return sdx_links

    def get_sdx_topology(self):
        topology = dict()
        topology["name"] = self.oxp_name
        topology["id"] = f"urn:sdx:topology:{self.oxp_url}"
        topology["version"] = self.version
        topology["timestamp"] = self.get_timestamp()
        topology["model_version"] = self.model_version
        topology["nodes"] = self.get_sdx_nodes()
        topology["links"] = self.get_sdx_links()
        topology["links"] += self.create_inter_oxp_link_entries()
        return topology
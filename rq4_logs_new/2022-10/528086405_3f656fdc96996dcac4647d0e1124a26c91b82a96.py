import pygraphviz as pgv

class ModelDiagram:
    """
    Class which can be used to make a diagram of dictionary models.
    The diagrams are made using pygraphviz https://pygraphviz.github.io/documentation/stable/ and
    the DOT language https://graphviz.org/doc/info/lang.html
    """
    
    def __init__(self, utils):
        """
        Constructor to create a ModelDiagram Object
        Parameters:
        ----------
        utils:Utils
            Instance of a utils class https://github.com/colav/UkuPacha/blob/main/ukupacha/Utils.py
        """
        self.graph_section_args = {"font_color": "gray66", "font_sz": "18", "water_mark": "CoLaV: https://github.com/colav",
                                   "node_sep": "0.5", "rank_dir": "LR", "rank_sep": "10.0"}
        self.node_section_args = {"font_sz": "12", "label": "\\N", "shape": "plaintext"}
        self.utils = utils
        self.colors = [
            '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b',
           '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']  # matplotlib TABLEAU_COLORS
        # https://www.color-hex.com/color-palette/1017697
        self.colors += ['#56ba5a', '#0cc6b8', "#fdc0c7", "#8b9df2"]
        # https://www.color-hex.com/color-palette/1017695
        self.colors += ["#0e8523", "#2b92eb", "#0911e0", "#7a00f9", "#b47ef4"]
        # https://www.color-hex.com/color-palette/1017694
        self.colors += ["#960735", "#f8102a", "#ff9807", "#fff84c", "#75f708"]
        # https://www.color-hex.com/color-palette/1017693
        self.colors += ["#986f42", "#50473f", "#554b50", "#423a28", "#8b7340"]
        self.colors += [
            '#00FFFF', '#7FFFD4', '#000000', '#0000FF', '#8A2BE2', '#A52A2A', '#DEB887',
            '#5F9EA0', '#7FFF00', '#D2691E', '#FF7F50', '#6495ED', '#DC143C', '#00FFFF',
            '#00008B', '#008B8B', '#B8860B', '#A9A9A9', '#006400', '#A9A9A9', '#BDB76B',
            '#8B008B', '#556B2F', '#FF8C00', '#9932CC', '#8B0000', '#E9967A', '#8FBC8F',
            '#483D8B', '#2F4F4F', '#2F4F4F', '#00CED1', '#9400D3', '#FF1493', '#00BFFF',
            '#696969', '#696969', '#1E90FF', '#B22222', '#228B22', '#FF00FF', '#FFD700',
            '#DAA520', '#808080', '#008000', '#ADFF2F', '#808080', '#FF69B4', '#CD5C5C',
            '#4B0082', '#F0E68C', '#7CFC00', '#ADD8E6', '#F08080', '#90EE90', '#FFB6C1',
            '#FFA07A', '#20B2AA', '#87CEFA', '#778899', '#00FF00', '#32CD32', '#FF00FF',
            '#800000', '#66CDAA', '#0000CD', '#BA55D3', '#9370DB', '#3CB371', '#7B68EE',
            '#00FA9A', '#48D1CC', '#C71585', '#191970', '#FFE4B5', '#FFDEAD', '#000080',
            '#808000', '#6B8E23', '#FFA500', '#FF4500', '#DA70D6', '#EEE8AA', '#98FB98',
            '#AFEEEE', '#DB7093', '#FFDAB9', '#CD853F', '#FFC0CB', '#DDA0DD', '#800080',
            '#663399', '#FF0000', '#BC8F8F', '#4169E1', '#8B4513', '#FA8072', '#F4A460',
            '#2E8B57', '#A0522D', '#C0C0C0', '#87CEEB', '#6A5ACD', '#708090', '#00FF7F',
            '#4682B4', '#D2B48C', '#008080', '#D8BFD8', '#FF6347', '#40E0D0', '#EE82EE',
            '#FFFF00', '#9ACD32']
        
    def get_table_desc(self, table_name):
        """
        Perform a request to get column names and their data datatype.
        Parameters:
        ----------
        table_name:str
            Name of the table which will be described
        Returns:
        ----------
            dataframe with the results
        """
        query = f"SELECT  column_name, data_type \
        FROM all_tab_columns where table_name='{table_name}'"
        return self.utils.request(query)
    
    def make_graph_metadata_section(self, font_color, font_sz, water_mark, node_sep, rank_dir, rank_sep):
        """
        Create a metadata section which has information that will be used to make the graph 
        https://graphviz.org/docs/graph/
        Parameters:
        ----------
        font_color:str
            Name of the color
        font_sz:float
            Size of the font
        water_mark:str
            Used to sign and identify the author of the graph
        node_sep:float
            Separation among nodes up to down
        rank_dir:str
            Defines how the graph is readed
        rank_sep:str
            Separtion among nodes left to right
        Returns:
        ----------
            string with DOT language format.
        """
        next_line = "\n"
        space = " "*4
        graph_section = f"{space}graph [{next_line}"
        graph_section += f"{space*2}fontcolor={font_color},{next_line}"
        graph_section += f"{space*2}fontsize={font_sz},{next_line}"
        graph_section += f"{space*2}label=\"{water_mark}\",{next_line}"
        graph_section += f"{space*2}nodesep={node_sep},{next_line}"
        graph_section += f"{space*2}rankdir={rank_dir},{next_line}"
        graph_section += f"{space*2}ranksep={rank_sep}{next_line}"
        graph_section += f"{space}];{next_line*2}"
        return graph_section
    
    def make_node_section(self, font_sz, label, shape):
        """
        Create a metadata section which has information that will be used to make the graph 
        https://graphviz.org/docs/nodes/
        Parameters:
        ----------
        font_sz:float
            Size of the font.
        label:str
            Text label attached to objects.
        shape:str
            Shape of the node.
        Returns:
        ----------
            string with DOT language format.
        """
        next_line = "\n"
        space = " "*4
        node_section = f"{space}node [{next_line}"
        node_section += f"{space*2}fontsize={font_sz},{next_line}"
        node_section += f"{space*2}label=\"{label}\",{next_line}"
        node_section += f"{space*2}shape={shape},{next_line}"
        node_section += f"{space}];{next_line*2}"
        return node_section

    def get_table_descs(self, graph):
        """
        Traverse the graph and get the table description of each node 
        Parameters:
        ----------
        graph:dict
            Dictionary which contains metadata and other nodes.
        Returns:
        ----------
            dictionary where each key is the name of the table and its value is the table description.
        """
        parent_node = graph[0]
        node_name = list(parent_node.keys())[0]
        node_name_no_alias = node_name.split("/")[0]
        sub_graphs = parent_node.get(node_name)
        if sub_graphs is None:
            table_desc = self.get_table_desc(node_name_no_alias).values.tolist()
            return {node_name: table_desc}
        table_descs = {}
        for sub_graph in sub_graphs:
            for node in sub_graph["TABLES"]:
                table_desc = self.get_table_descs([node])
                for key in table_desc:
                    if not key in list(table_descs.keys()):
                        table_descs[key] = table_desc[key]
        table_descs[node_name] = self.get_table_desc(node_name).values.tolist()
        return table_descs

    def get_key_attributes(self, graph, key_attributes):
        """
        Traverse the graph and get the names of the attributes that will be used in
        the KEY section of the dictionary.
        Parameters:
        ----------
        graph:dict
            Dictionary which contains metadata and other nodes.
        key_attributes:dict
            Dictionary where each key is the name of the table and its value is a set of attributes.
        """
        parent_node = graph[0]
        parent_node_name = list(parent_node.keys())[0]
        sub_graphs = parent_node.get(parent_node_name)
        if sub_graphs is None:
            return
        if not parent_node_name in list(key_attributes.keys()):
            key_attributes[parent_node_name] = set()
        for sub_graph in sub_graphs:
            db = sub_graph["DB"]
            parent_key_attributes = list(map(lambda key: key[:key.find('/')] if '/' in key else key,
                                             sub_graph["KEYS"]))
            for parent_key_attribute in parent_key_attributes:
                key_attributes[parent_node_name].add(parent_key_attribute)
            node_key_attributes = list(map(lambda key: key[key.find('/')+1:] if '/' in key else key,
                                           sub_graph["KEYS"]))
            for node in sub_graph["TABLES"]:
                node_name = list(node.keys())[0]
                if not node_name in list(key_attributes.keys()):
                    key_attributes[node_name] = set()
                for node_key_attribute in node_key_attributes:
                    key_attributes[node_name].add(node_key_attribute)
                self.get_key_attributes([node], key_attributes)
    
    def make_table(self, db, name, table_desc, key_attributes):
        """
        Make a string containing DOT language instructions to create a table.
        Parameters:
        ----------
        db:str
            Database where the table is stored.
        name:str
            Name of the table.
        table_desc:dataframe
            Description of the table column names and their types.
        key_attributes:dict
            Attributes from the table that are used on the KEY section of the model
        Returns:
        ----------
            string containing DOT language instructions to create a table.
        """
        next_line = "\n"
        space = " "*4
        ID = f"{db}_{name}"
        table = f"{space}\"{ID}\" [{next_line}"
        table += f"{space*2}label={next_line}"
        table += f"{space*2}<<table border=\"0\" cellborder=\"1\" cellspacing=\"0\">{next_line}"
        table += f"{space*3}<tr>{next_line}"
        table += f"{space*4}<td>{next_line}"
        table += f"{space*5}<b>{ID}</b>{next_line}"
        table += f"{space*4}</td>{next_line}"
        table += f"{space*3}</tr>{next_line}"
        for attribute, db_type in table_desc:
            table += f"{space*3}<tr>"
            if attribute in key_attributes:
                table += f"<td align=\"center\" port=\"{attribute}\">"
            else:
                table += f"<td align=\"center\">"
            table += f"{attribute} :: {db_type}</td>{next_line}"
            table += f"{space*3}</tr>{next_line}"
        table += f"{space*2}</table>>{next_line}"
        table += f"{space}];{next_line*2}"
        return table
    
    def make_table_section(self, graph, pdb, table_descs, key_attributes, visited_nodes):
        """
        Traverse the graph and make a section where the tables are defined.
        Parameters:
        ----------
        graph:dict
            Dictionary which contains metadata and other nodes.
        pdb:str
            Database where the table is stored.
        table_desc:dataframe
            Description of the table column names and their types.
        key_attributes:dict
            Attributes from the table that are used on the KEY section of the model.
        visited_nodes:list
            List which contains the nodes that already have a table created on DOT language,
            this way repetition is avoided.
        Returns:
        ----------
            string containing DOT language instructions to create several tables.
        """
        parent_node = graph[0]
        parent_node_name = list(parent_node.keys())[0]
        if not parent_node_name in visited_nodes:
            visited_nodes.append(parent_node_name)
        else:
            return ""
        sub_graphs = parent_node.get(parent_node_name)
        if sub_graphs is None:
            return self.make_table(pdb, parent_node_name, table_descs[parent_node_name],
                                   key_attributes[parent_node_name])
        table_section = ""
        for sub_graph in sub_graphs:
            db = sub_graph["DB"]
            for node in sub_graph["TABLES"]:
                table_section += self.make_table_section([node], db, table_descs, key_attributes, visited_nodes)
        table_section += self.make_table(pdb, parent_node_name, table_descs[parent_node_name],
                                         key_attributes[parent_node_name])
        return table_section

    def make_graph_section(self, graph, pdb, colors):
        """
        Traverse the graph and make a section where nodes and edges are created.
        Parameters:
        ----------
        graph:dict
            Dictionary which contains metadata and other nodes.
        pdb:str
            Database where the table is stored.
        colors:list
            List of colors that will be given to edges.
        Returns:
        ----------
            string containing DOT language instructions to create nodes ans their edges.
        """
        parent_node = graph[0]
        parent_node_name = list(parent_node.keys())[0]
        sub_graphs = parent_node.get(parent_node_name)
        if sub_graphs is None:
            return ""
        next_line = "\n"
        space = " "*4
        accum_graph_section = ""
        graph_section = ""
        for sub_graph in sub_graphs:
            db = sub_graph["DB"]
            keys = sub_graph["KEYS"]
            for node in sub_graph["TABLES"]:
                color = colors.pop(0)
                if not colors:
                    colors = self.colors.copy()
                node_name = list(node.keys())[0]
                parent_keys = list(map(lambda key: key[:key.find('/')] if '/' in key else key, keys))
                node_keys = list(map(lambda key: key[key.find('/')+1:] if '/' in key else key, keys))
                for key in keys:
                    parent_attribute = key[:key.find("/")] if '/' in key else key
                    node_attribute = key[key.find("/")+1:] if '/' in key else key
                    graph_section += f"{space}\"{pdb}_{parent_node_name}\":{parent_attribute}:e"
                    graph_section += " -> "
                    graph_section += f"\"{db}_{node_name}\":{node_attribute}:w "
                    graph_section  += f"[arrowhead=nonenormal color=\"{color}\"];{next_line}"
                accum_graph_section += self.make_graph_section([node], db, colors)
        accum_graph_section = graph_section + accum_graph_section
        return accum_graph_section
            
    def make_dot_program(self, model, diagram_name):
        """
        Create a dot program from a model.
        Parameters:
        ----------
        model:dict
            Dictionary which contains metadata and other nodes.
        diagram_name:str
            Name of the diagram.
        Returns:
        ----------
            string containing DOT language instructions to create a digraph.
        """
        next_line = "\n"
        space = " "*4
        dot_program = f"digraph \"{diagram_name}\" "+"{" 
        dot_program += f"{next_line*2}"
        dot_program += self.make_graph_metadata_section(**self.graph_section_args)
        dot_program += self.make_node_section(**self.node_section_args)
        graph = model["GRAPH"]
        db = model["CHECKPOINT"]["DB"]
        table_descs = self.get_table_descs(graph)
        key_attributes = {}
        self.get_key_attributes(graph, key_attributes)
        dot_program += f"{space}//Table Section{next_line*2}"
        dot_program += self.make_table_section(graph, db, table_descs, key_attributes, [])
        dot_program += f"{space}//End Table Section{next_line*2}"
        colors = self.colors.copy()
        dot_program += f"{space}//Graph Section{next_line*2}"
        dot_program += self.make_graph_section(graph, db, colors)
        dot_program += f"{space}//End Graph Section{next_line*2}"
        dot_program += "}"
        return dot_program
    
    def make_diagram(self, model, name, extension="svg"):
        """
        Create a diagram from a dot program using pygraphviz.
        Parameters:
        ----------
        model:dict
            Dictionary which contains metadata and other nodes.
        diagram_name:str
            Name of the diagram.
        extension:str
            File extension for the diagram.
        """
        dot_program = self.make_dot_program(model, name)
        G = pgv.AGraph(dot_program)
        G.layout(prog="dot")
        G.draw(f"{name}.{extension}")
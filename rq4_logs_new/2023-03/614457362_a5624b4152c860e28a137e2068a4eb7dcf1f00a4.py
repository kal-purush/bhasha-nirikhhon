import networkx as nx
import matplotlib.pyplot as plt
from matplotlib.widgets import RadioButtons, Button
import xml.etree.ElementTree as ET

class GraphPlotter:
    def __init__(self):
        self.G = nx.Graph()
        self.G.add_nodes_from(['A', 'B', 'C', 'D'])
        self.G.add_edges_from([('A', 'B'), ('B', 'C'), ('C', 'D'), ('D', 'A')])

        self.fig, self.ax = plt.subplots(figsize=(12, 8))
        plt.subplots_adjust(left=0.3)

        self.fixed_pos = {'A': (-0.5, 0.5), 'B': (0.5, 0.5), 'C': (0.5, -0.5), 'D': (-0.5, -0.5)}
        self.pos = self.fixed_pos.copy()

        self.radio_ax = plt.axes([0.05, 0.7, 0.15, 0.15])
        self.radio = RadioButtons(self.radio_ax, ('Add node', 'Connect nodes'))

        self.undo_button_ax = plt.axes([0.05, 0.5, 0.15, 0.075])
        self.undo_button = Button(self.undo_button_ax, 'Undo', color='red', hovercolor='lightcoral')

        self.current_mode = 'Add node'
        self.last_clicked_node = None
        self.previous_states = []

        self.fig.canvas.mpl_connect('button_press_event', self.on_click)
        self.radio.on_clicked(self.update_mode)
        self.undo_button.on_clicked(self.undo_step)

    def draw_graph(self):
        self.ax.clear()
        nx.draw(self.G, self.pos, with_labels=True, font_weight='bold', node_color='orange', node_size=1000, ax=self.ax)
        plt.axis('off')
        self.fig.canvas.draw()

    def on_click(self, event):
        x, y = event.xdata, event.ydata

        if x is not None and y is not None:
            closest_node = None
            min_dist = float('inf')
            for node, (node_x, node_y) in self.pos.items():
                dist = ((x - node_x) ** 2 + (y - node_y) ** 2) ** 0.5
                if dist < min_dist:
                    min_dist = dist
                    closest_node = node

            self.previous_states.append((self.G.copy(), self.pos.copy()))

            if self.current_mode == 'Add node':
                node_label = f"X{len(self.G.nodes) - 4}"
                self.G.add_node(node_label)
                self.pos[node_label] = (x, y)
                self.draw_graph()
            elif self.current_mode == 'Connect nodes':
                if self.last_clicked_node is None:
                    self.last_clicked_node = closest_node
                else:
                    self.G.add_edge(self.last_clicked_node, closest_node)
                    self.last_clicked_node = None
                    self.draw_graph()

    def update_mode(self, label):
        self.current_mode = label

    def undo_step(self, event):
        if len(self.previous_states) > 0:
            self.G, self.pos = self.previous_states.pop()
            self.draw_graph()

    def export_graph_to_xml(self, file_name):
        graph_element = ET.Element('graph')

        for node in self.G.nodes():
            node_element = ET.SubElement(graph_element        , 'node')
            node_element.set('id', str(node))
            x, y = self.pos[node]
            node_element.set('x', str(x))
            node_element.set('y', str(y))

        for edge in self.G.edges():
            edge_element = ET.SubElement(graph_element, 'edge')
            edge_element.set('source', str(edge[0]))
            edge_element.set('target', str(edge[1]))

        tree = ET.ElementTree(graph_element)
        tree.write(file_name, encoding='utf-8', xml_declaration=True)

    def show(self):
        self.draw_graph()
        plt.show()
from graphviz import Digraph

__all__ = ['Uml']


class Uml(Digraph):

    def __init__(self, name=None, comment=None,
                 filename=None, directory=None,
                 format=None, engine=None,
                 graph_attr=None, node_attr=None, edge_attr=None, body=None,
                 strict=False):
        '''
        if not graph_attr:
            graph_attr = {
                'fontname': "Microsoft YaHei"
            }
        else:
            graph_attr['fontname'] = "Microsoft YaHei"
        graph_attr['rankdir'] = 'BT'

        if not node_attr:
            node_attr = {
                'fontname': 'Microsoft YaHei'
            }
        else:
            node_attr['fontname'] = 'Microsoft YaHei'

        if not edge_attr:
            edge_attr = {
                'fontname': 'Microsoft YaHei',
                'fontsize': '8'
            }
        else:
            edge_attr['fontname'] = 'Microsoft YaHei'
        '''
        
        super(Uml, self).__init__(name=name, comment=comment,
                                  filename=filename, directory=directory,
                                  format=format, engine=engine,
                                  graph_attr=graph_attr, node_attr=node_attr, edge_attr=edge_attr, body=body,
                                  strict=strict)

    def rect(self, name, label=None):
        if not label:
            label = name
        self.node(name=name, label=label, shape='rectangle')

    def klass(self, name, color='steelblue', label_name=None, attrs=[], methods=[]):
        if not label_name:
            label_name = name
        label = '{' + label_name + '\l|'
        for a in attrs:
            label += '- ' + a + '\l'
        label += '|'
        for m in methods:
            label += '+ ' + m + '\l'
        label += '}'
        self.node(name=name, label=label, shape='record', color=color)

    def inf(self, name, attrs=[], methods=[]):
        label_name = '\<\<inf\>\>\l ' + name
        self.klass(name, 'seagreen', label_name, attrs, methods)

    def deri(self, head, tail, color=None):
        self.edge(head, tail, arrowhead='onormal', label='inherited\nIs a ...', color='lightcoral', fontcolor='lightcoral')

    def impl(self, head, tail):
        self.edge(head, tail, style='dashed', arrowhead='onormal', label='Implement', color='seagreen', fontcolor='seagreen')

    def comp(self, head, tail):
        self.edge(head, tail, arrowhead='diamond', label='Composition\nIs part of ...', color='tan', fontcolor='tan')

    def aggr(self, head, tail):
        self.edge(head, tail, arrowhead='odiamond', label='Aggregation\nOwns a ... ', color='darkseegreen', fontcolor='darkseegreen')

    def asso(self, head, tail):
        self.edge(head, tail, arrowhead='vee', label='Association\nHas a ...', color='sandybrown', fontcolor='sandybrown')

    def dep(self, head, tail):
        self.edge(head, tail, style='dashed', arrowhead='vee', label='Dependency\nUses a ...', color='salmon', fontcolor='salmon')


dot = Uml(name='UML_Class_Demo', comment='Demostrate UML class relationships')

'''
dot.graph_attr['fontname'] = "Bitstream Vera Sans"
dot.graph_attr['fontsize'] = '6'
dot.edge_attr = {'fontname': "Bitstream Vera Sans"}
'''

# Example

dot.klass(name='Animal', attrs=['name : string', 'age : int'])
dot.inf(name='AnimalBehavior', methods=['voice() : void', 'die() : void'])
dot.klass(name='Dog')
dot.klass(name='Cat')

dot.deri('Dog', 'Animal')
dot.impl('Dog', 'AnimalBehavior')
dot.deri('Cat', 'Animal')
dot.impl('Cat', 'AnimalBehavior')
dot.comp('AnimalBehavior', 'Animal')

dot.klass(name='Water')
dot.dep('Dog', 'Water')

dot.klass(name='DogGroup')
dot.aggr('Dog', 'DogGroup')

dot.klass(name='Guard')
dot.asso('Dog', 'Guard')

# Output

print(dot.source)
dot.render('output/uml-demo.gv', view=True)
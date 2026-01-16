class HTMLNode():
    def __init__(self, tag=None, value=None, children=None, props=None):
        # if isinstance(tag, str) != True and tag !=  None:
        #     raise Exception("HTMLNode arguments are faulty. Please follow this order: (tag=string, value=string, children=list, props=dict)")
        # if isinstance(value, str) != True and value !=  None:
        #     raise Exception("HTMLNode arguments are faulty. Please follow this order: (tag=string, value=string, children=list, props=dict)")
        # if isinstance(children, list) != True and children !=  None:
        #     raise Exception("HTMLNode arguments are faulty. Please follow this order: (tag=string, value=string, children=list, props=dict)")
        # if isinstance(props, dict) != True and props != None:
        #     raise Exception("HTMLNode arguments are faulty. Please follow this order: (tag=string, value=string, children=list, props=dict)")

        self.tag = tag
        self.value = value
        self.children = children
        self.props = props
        
    def __repr__(self):
        return f'HTMLNode({self.tag}, {self.value}, {self.children}, {self.props})'

    def print_details(self):
        print(f"Tags: {self.tag}")
        if self.value == None:
            print(f"Value: {self.value}")
        elif len(self.value) < 21:
            print(f"Value: {self.value[0:10]}")
        else:
            print(f"Value: {self.value[0:10]} ...{self.value[-20:len(self.value)]}")
        print(f"Children: {self.children}")
        print(f"Props: {self.props}")
            

    def to_html(self):
        raise NotImplementedError
    def props_to_html(self):
        prop = ""
        if self.props is None:
            return prop
        for key in self.props:
            prop += f' {key}="{self.props[str(key)]}"'
        return prop
    
class LeafNode(HTMLNode):
    def __init__(self, tag=None, value=None, props=None):
        super().__init__(tag, value, None, props)
    
    def to_html(self):
        if self.value == None:
            raise ValueError
        elif self.tag == None:
            return self.value
        return f'<{self.tag}{self.props_to_html()}>{self.value}</{self.tag}>'

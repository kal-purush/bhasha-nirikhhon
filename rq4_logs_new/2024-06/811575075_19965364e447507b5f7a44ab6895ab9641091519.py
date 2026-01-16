from textnode import TextNode
from typing import List
from enum import Enum

class TextType(Enum):
    TEXT = "text"
    BOLD = "bold"
    ITALIC = "italic"
    CODE = "code"
    LINK = "link"
    IMAGE = "image"

DELIMITER_TO_TYPE = {
    '**': TextType.BOLD,
    '__': TextType.BOLD,
    '*': TextType.ITALIC,
    '_': TextType.ITALIC,
    '`': TextType.CODE,
}

#------------New split node delimiter------------#
def delimiter_loop_on_textnodes(nodes: List[TextNode]) -> List[TextNode]:
    """
    For each delimiter, send the list of TextNodes to textnodes_loop_with_delimiter(nodes, delimiter)\n
    textnodes_loop_with_delimiter will return a new list of nodes separated by the given delimiter and return a new list\n
    This is will go until all the delimiters are accounted for.\n
    At the end, this will return a fully and properly delimited set of TextNodes including nested ones.
    """ 
    for delimiter in DELIMITER_TO_TYPE:
        nodes = textnodes_loop_with_delimiter(nodes, delimiter)
    # print(nodes)
    return nodes

def textnodes_loop_with_delimiter(nodes: List[TextNode], delimiter):
    """
    Takes a list of TextNodes and a delimiter. Will send each node in the list and the given delimiter to the split_node_delimiter function and will extend that result to a new list of nodes.\n
    """
    new_nodes = []
    for node in nodes:
    #------unordered list exception-----#
        if delimiter == "*" and node.text[0:2] == "* ":
            unordered_list_text = node.text[2:]
            unordered_list_nodes = split_node_delimiter(TextNode(unordered_list_text, TextType.TEXT), delimiter)
            if unordered_list_nodes[0].text is not None:
                unordered_list_nodes[0].text = "* " + unordered_list_nodes[0].text
                new_nodes.extend(unordered_list_nodes)
            else:
                unordered_list_nodes[1].text = "* " + unordered_list_nodes[1].text
                new_nodes.extend(unordered_list_nodes)
    #------unordered list exception-----#
        else:
            new_nodes.extend(split_node_delimiter(node, delimiter))    
    return new_nodes

def split_node_delimiter(node: TextNode, delimiter) -> List[TextNode]:
    """
    Take a TextNode and a delimiter, check the text of the TextNode against the delimiter and if it contains it, split the text into multiple new textnodes\n
    ## Example:\n
        node = TextNode("This is text with a `code block` word", "text")
        ### new_nodes will be:
        #### [
        ####     TextNode("This is text with a ", "text"),
        ####     TextNode("code block", "code"),
        ####    TextNode(" word", "text"),
        #### ]
    """
    i = 0
    x = 0
    new_nodes = []
    text = node.text
    if node.text_type != TextType.TEXT:
        new_nodes.append(node)
    elif delimiter in text:
        # Set i to the first instance index of the delimiter
        i = text.index(delimiter)
        # If there is no match for the delimiter in the rest of the text then its not a true case of that delimiter
        # and append it as is
        if delimiter not in text[i+len(delimiter):]:
            new_nodes.append(node)
        # If i is not at the start of the text then make that slice from 0 - i of the text a new TextNode and append it to the list.
        if i != 0:
            new_nodes.append(TextNode(text[:i], TextType.TEXT))
        # Set x to the value of the next index where you find the delimiter
        x = text[i+len(delimiter):].index(delimiter) + len(text[:i+len(delimiter)])
        x += len(delimiter)
        # Check if x is the outside delimiter, and if its not, make it.
        if x != len(text) and text[x] == delimiter[0]:
            x += 1 
        # Send the delimited text to the splitter, and extend the results into new_nodes
        new_nodes.extend(splitter(text[i:x], delimiter))
        # If its not the end of the node's text, send the remainder back through this function.
        if x != len(text):
            new_nodes.extend(split_node_delimiter(TextNode(text[x:], TextType.TEXT), delimiter))
    else:
        new_nodes.append(node)
     
    return new_nodes
        
def splitter(text: str, delimiter) -> List[TextNode]:
    """
    Take a text string that has a delimiter in it (already determined by the caller) and split off the ends, creating a 3 part list.\n
    ### Example:
        ["**", "This is bolded text", "**"]\n
    Send the middle text portion of the split to the nest_checker to see if there are any more delimiters in it.\n
    If there's no nested delimiter, return a single item list containing a TextNode with the text and the TextType matching the delimiter.\n
    ### returns:
        [TextNode("This is bolded text", TextType.BOLD)]\n
    If there is a nested delimiter, return 3 textnodes, containing empty text delimiter-typed nodes wrapping a node with the rest of the text and TextType.TEXT.\n
    ### Example:
        ["**", "This is bolded and *italicized* text", "**"]
    ### returns:
        [
        #### TextNode(None, TextType.BOLD),
        #### TextNode("This is bolded and *italicized* text", TextType.TEXT),
        #### TextNode(None, TextType.BOLD)
        ]
    """
    new_nodes = []
    split_text = []
    split_text.append(text.partition(delimiter)[1])
    split_text.append(text.partition(delimiter)[2].rpartition(delimiter)[0])
    split_text.append(text.partition(delimiter)[2].rpartition(delimiter)[1])
    if nest_checker(split_text[1]) is False:
        new_nodes.append(TextNode(split_text[1], DELIMITER_TO_TYPE.get(delimiter)))
        return new_nodes
    else:
        new_nodes.append(TextNode("", DELIMITER_TO_TYPE.get(delimiter)))
        new_nodes.append(TextNode(split_text[1], TextType.TEXT))
        new_nodes.append(TextNode("", DELIMITER_TO_TYPE.get(delimiter)))
    return new_nodes

def nest_checker(text):
    for delim in DELIMITER_TO_TYPE:
        if delim in text:
            return True
    return False
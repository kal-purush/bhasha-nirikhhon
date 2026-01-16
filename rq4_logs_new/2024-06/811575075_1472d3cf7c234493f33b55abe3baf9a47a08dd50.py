from textnode import (TextNode, TextType, type_rules, delimiters, delimiters2, DELIMITER_TO_TYPE)
from typing import Optional, List
from enum import Enum
import re

#-----------------------------Inline Markdown------------------------------#
def split_node_delimiter(old_node: TextNode, delimiter: str) -> List[TextNode]:
    if old_node.text_type != TextType.TEXT:
        return [old_node]
    new_nodes = []
    split = old_node.text.split(delimiter)
    for i, item in enumerate(split):
        # if len(split) > 1 and "*" in split[i] and "*" in split[i+1]:
        #     item += "*"
        #     split[i+1] = split[i+1][1:]
        if item == "":
            continue
        if i % 2 == 0:
            new_nodes.append(TextNode(item, TextType.TEXT))
        else:
            new_nodes.append(TextNode(item, DELIMITER_TO_TYPE.get(delimiter)))
    return new_nodes

def split_nodes_delimiter(old_nodes: List[TextNode], delimiter: str) -> List[TextNode]:
    new_nodes: List[TextNode] = []
    for node in old_nodes:
        new_nodes.extend(split_node_delimiter(node, delimiter))
    return new_nodes

def nested_nodes_unpacker(old_nodes: List[TextNode]):
    unpacked_nodes: List[TextNode] = []
    for node in old_nodes:
        if node.text_type == TextType.TEXT:
            unpacked_nodes.append(node)
        else:
            unpacked_nodes.extend(nest_checker(node))
    return unpacked_nodes

def nest_checker(node: TextNode):
    new_nodes: List[TextNode] = []
    if split_nodes_by_delimiters([TextNode(node.text, TextType.TEXT)])[0].text_type != TextType.TEXT and split_nodes_by_delimiters([TextNode(node.text, TextType.TEXT)])[0].text_type != node.text_type:
        new_nodes.append(TextNode("", node.text_type))
        new_nodes.extend(split_nodes_by_delimiters([TextNode(node.text, TextType.TEXT)]))
        new_nodes.append(TextNode("", node.text_type))
    else:
        new_nodes.append(node)
    return new_nodes

def split_nodes_by_delimiters(nodes: List[TextNode]):
    for delimiter in DELIMITER_TO_TYPE:
        nodes = split_nodes_delimiter(nodes, delimiter)
    return nested_nodes_unpacker(nodes)

def extract_markdown_images(text) -> List[tuple]:
    return re.findall(r"!\[(.*?)\]\((.*?)\)", text)

def extract_markdown_links(text) -> List[tuple]:
    return re.findall(r"\[(.*?)\]\((.*?)\)", text)

def split_nodes_image(old_nodes: List[TextNode]) -> List[TextNode]:
    new_nodes = []
    image_pattern = re.compile(r"!\[(.*?)\]\((.*?)\)")
    for node in old_nodes:
        if image_pattern.search(node.text) == None:
            if node.text_type == TextType.IMAGE:
                node.text_type = TextType.TEXT
            new_nodes.append(node)
        else:
            images = extract_markdown_images(node.text)
            split = node.text.split(f"![{images[0][0]}]({images[0][1]})", 1)
            if bool(split[0]) == True:
                new_nodes.append(TextNode(split[0], TextType.TEXT))
            new_nodes.append(TextNode(images[0][0], TextType.IMAGE, images[0][1]))
            if len(split) > 1 and bool(split[1]) == True:
                new_nodes.extend(split_nodes_image([TextNode(split[1], TextType.IMAGE)]))
    return new_nodes
            
def split_nodes_link(old_nodes: List[TextNode]) -> List[TextNode]:
    new_nodes = []
    link_pattern = re.compile(r"\[(.*?)\]\((.*?)\)")
    for node in old_nodes:
        if link_pattern.search(node.text) == None:
            if node.text_type == TextType.LINK:
                node.text_type = TextType.TEXT
            new_nodes.append(node)
        else:
            links = extract_markdown_links(node.text)
            split = node.text.split(f"[{links[0][0]}]({links[0][1]})", 1)
            if bool(split[0]) == True:
                new_nodes.append(TextNode(split[0], TextType.TEXT))
            new_nodes.append(TextNode(links[0][0], TextType.LINK, links[0][1]))
            if len(split) > 1 and bool(split[1]) == True:
                new_nodes.extend(split_nodes_link([TextNode(split[1], TextType.LINK)]))
    return new_nodes

def text_to_textnodes(text) -> List[TextNode]:
    node = TextNode(text, TextType.TEXT)
    delimited = split_nodes_by_delimiters([node])
    split_image = split_nodes_image(delimited)
    split_link = split_nodes_link(split_image)
    return split_link
  
#print(text_to_textnodes("This is **text** with an *italic* word and a `code block` and an ![image](https://storage.googleapis.com/qvault-webapp-dynamic-assets/course_assets/zjjcJKZ.png) and a [link](https://boot.dev)"))

class MKBlockType(Enum):
    Paragraph = "paragraph"
    Heading = "heading"
    Code = "code"
    Quote = "quote"
    UnorderedList = "unordered_list"
    OrderedList = "ordered_list"

def markdown_to_blocks(markdown: str):
    blocks = markdown.split("\n\n")
    block_list = []
    for block in blocks:
        if block == "":
            continue
        block_list.append(block.strip())
    return block_list
with open("/home/danlern2/bootdevworkspace/static_site/src/markdown_test") as file:
    markdown_to_blocks(file.read())

def block_to_block_type(markdown_block: str) -> MKBlockType:
    if (
        markdown_block.startswith("# ")
        or markdown_block.startswith("## ")
        or markdown_block.startswith("### ")
        or markdown_block.startswith("#### ")
        or markdown_block.startswith("##### ")
        or markdown_block.startswith("###### ")
    ):
        return MKBlockType.Heading
    elif markdown_block[0:3] == "```" and markdown_block[-3:len(markdown_block)] == "```":
        return MKBlockType.Code
    elif markdown_block[0] == ">" and len(set([item[0] for item in markdown_block.split("\n")])) == 1:
        return MKBlockType.Quote
    elif (markdown_block[0] == "-" or markdown_block[0] == "*") and set([item[0:2] for item in markdown_block.split("\n")]) <= set(["- ", "* "]):
        return MKBlockType.UnorderedList
    elif markdown_block.startswith("1. "):
        i = 1
        for line in markdown_block.split("\n"):
            if not line.startswith(f"{i}. "):
                return MKBlockType.Paragraph
            i += 1
        return MKBlockType.OrderedList
    return MKBlockType.Paragraph

print(block_to_block_type(">quote 1\n>quote 2\n>quote 3"))
print(block_to_block_type("```this is a code block```"))
print(block_to_block_type("- This is\n* an\n* unordered_list"))
print(block_to_block_type("1. This is\n2. an\n3. ordered_list"))
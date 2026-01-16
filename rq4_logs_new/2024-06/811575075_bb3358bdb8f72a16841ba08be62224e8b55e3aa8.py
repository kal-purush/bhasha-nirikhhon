from __future__ import annotations
from htmlnode_class import (
    HTMLNode,
    GrandparentNode,
    LeafNode,
    ParentNode,
)
from markdown_direct_to_html import (
    DELIMITER_TO_TYPE,
    DelimiterType,
    # DELIM_TO_RULE,
)


# def get_rule(delim):
#     from markdown_direct_to_html import DELIM_TO_RULE
#     return DELIM_TO_RULE[delim]


def delimiter_nester_in_node(grandparent_node: GrandparentNode, text: str):
    children: list[HTMLNode] = grandparent_node.children
    result = base_rule(text)
    children.extend(result[0])
    # children.append(LeafNode(result[1]))
    return grandparent_node


def base_rule(text: str) -> tuple[list[HTMLNode], str]:
    children: list[HTMLNode] = []
    plain_text: str = ""

    if len(text) == 0:
        return children, plain_text

    for i in range(len(text)):
        for delim in DELIMITER_TO_TYPE:
            if text[i:].startswith(delim):
                plain_text = text[:i]
                # Since a delimiter rule was called, there is no more plain text. take what you have and append it as a leafnode
                children.append(LeafNode(plain_text))
                # Reset plain text so it returns blank.
                plain_text = ""
                # Make a new parentnode with the tag type of the delimiter
                delim_parent = ParentNode(DELIMITER_TO_TYPE.get(delim), children=[])
                # Call the delimiter specific rule on all the remaining text except the delimiter itself
                rule_result = DELIM_TO_RULE[delim](text[i + len(delim) :])
                # Add whatever nested children were made in the rule to the delimiter parent's children list
                delim_parent.children.extend(rule_result[0])
                # Recursive call on the remaining text returned from the rule
                rest_of_text_result = base_rule(rule_result[1])
                plain_text += rest_of_text_result[1]
                children.append(delim_parent)
                children.extend(rest_of_text_result[0])
                return children, plain_text

    children.append(LeafNode(text))
    print(f"From base: {children}")
    # return children, plain_text
    return children, text


def italic_rule(text: str) -> tuple[list[HTMLNode], str]:
    children: list[HTMLNode] = []
    plain_text: str = ""
    print(f"top italic = {text}")
    if len(text) == 0:
        return children, plain_text

    for i in range(len(text)):
        for delim in DELIMITER_TO_TYPE:
            if text[i:].startswith(delim) and (delim == "*" or delim == "_"):
                print("hi")
                plain_text = text[:i]
                children.append(LeafNode(plain_text))
                return children, text[i + len(delim) :]

            elif text[i:].startswith(delim):
                plain_text = text[:i]
                # Since a delimiter rule was called, there is no more plain text. take what you have and append it as a leafnode
                children.append(LeafNode(plain_text))
                # Reset plain text so it returns blank.
                plain_text = ""
                # Make a new parentnode with the tag type of the delimiter
                delim_parent = ParentNode(DELIMITER_TO_TYPE.get(delim), children=[])
                # Call the delimiter specific rule on all the remaining text except the delimiter itself
                rule_result = DELIM_TO_RULE[delim](text[i + len(delim) :])
                # Add whatever nested children were made in the rule to the delimiter parent's children list
                delim_parent.children.extend(rule_result[0])
                children.append(delim_parent)
                # Recursive call on the remaining text returned from the rule
                rest_of_italic = italic_rule(rule_result[1])
                children.extend(rest_of_italic[0])
                print(f"from italic 2{children}")
                return children, rest_of_italic[1]
    children.append(LeafNode(text))
    print(f"From italic: {children}")
    return children, plain_text


def bold_rule(text: str) -> tuple[list[HTMLNode], str]:
    children: list[HTMLNode] = []
    plain_text: str = ""
    print(f"top bold = {text}")
    if len(text) == 0:
        return children, plain_text

    for i in range(len(text)):
        for delim in DELIMITER_TO_TYPE:
            if text[i:].startswith(delim) and (delim == "**" or delim == "__"):
                print("bold ending")
                plain_text = text[:i]
                children.append(LeafNode(plain_text))
                return children, text[i + len(delim) :]

            elif text[i:].startswith(delim):
                plain_text = text[:i]
                # Since a delimiter rule was called, there is no more plain text. take what you have and append it as a leafnode
                children.append(LeafNode(plain_text))
                # Reset plain text so it returns blank.
                plain_text = ""
                # Make a new parentnode with the tag type of the delimiter
                delim_parent = ParentNode(DELIMITER_TO_TYPE.get(delim), children=[])
                # Call the delimiter specific rule on all the remaining text except the delimiter itself
                rule_result = DELIM_TO_RULE[delim](text[i + len(delim) :])
                # Add whatever nested children were made in the rule to the delimiter parent's children list
                delim_parent.children.extend(rule_result[0])
                children.append(delim_parent)
                # Recursive call on the remaining text returned from the rule
                rest_of_bold = italic_rule(rule_result[1])
                children.extend(rest_of_bold[0])
                print(f"from bold 2{children}")
                return children, rest_of_bold[1]
    children.append(LeafNode(text))
    print(f"From bold: {children}")
    return children, plain_text


def strike_rule(text: str) -> tuple[list[HTMLNode], str]:
    children: list[HTMLNode] = []
    plain_text: str = ""
    pass


def code_rule(text: str) -> tuple[list[HTMLNode], str]:
    children: list[HTMLNode] = []
    plain_text: str = ""
    pass


def image_rule(text: str) -> tuple[list[HTMLNode], str]:
    # image_node: HTMLNode = ParentNode("img", children=list[HTMLNode], props={"src": "", "alt": ""}) # props={"src": /url/, "alt": /the plain text/}
    children: list[HTMLNode] = []
    plain_text: str = ""
    url: LeafNode = LeafNode("")
    pass


def link_rule(text: str) -> tuple[list[HTMLNode], str]:
    # link_node: HTMLNode = ParentNode("a", children=list[HTMLNode], props={"href": }) # props={"href": /url/}
    children: list[HTMLNode] = []
    plain_text: LeafNode = LeafNode("")
    url: LeafNode = LeafNode("")
    pass


DELIM_TO_RULE: dict[str,] = {
    "**": bold_rule,
    "__": bold_rule,
    "~~": strike_rule,
    "*": italic_rule,
    "_": italic_rule,
    "[": link_rule,
    "!": image_rule,
}
# return delimiter_nester_in_node()


node = GrandparentNode("pre", children=[])
text = "This is an *italic-containing sentence, **and also has** bold*"

print(delimiter_nester_in_node(node, text))
from __future__ import annotations
from delimiter_rules import delimiter_nester_in_node
from imports import TagType
from htmlnode_class import HTMLNode, LeafNode, ParentNode


def markdown_to_blocks(markdown: str) -> list[str]:
    blocks = markdown.split("\n\n")
    block_list: list[str] = []
    for block in blocks:
        if block == "":
            continue
        block_list.append(block.strip())
    return block_list


def block_to_block_type(markdown_block: str):
    if (
        markdown_block.startswith("# ")
        or markdown_block.startswith("## ")
        or markdown_block.startswith("### ")
        or markdown_block.startswith("#### ")
        or markdown_block.startswith("##### ")
        or markdown_block.startswith("###### ")
    ):
        return TagType.HEADING
    elif (
        markdown_block[0:3] == "```"
        and markdown_block[-3 : len(markdown_block)] == "```"
    ):
        return TagType.CODEBLOCK
    elif (
        markdown_block[0] == ">"
        and len(set([item[0] for item in markdown_block.split("\n")])) == 1
    ):
        return TagType.QUOTE
    elif (
        markdown_block[0] == "-"
        or markdown_block[0] == "*"
        or markdown_block[0] == "+"
        or markdown_block[0] == "-"
    ) and set([item[0:2] for item in markdown_block.split("\n")]) <= set(
        ["- ", "* ", "+ ", "- "]
    ):
        return TagType.UNOLIST
    elif markdown_block.startswith("1. "):
        i = 1
        for line in markdown_block.split("\n"):
            if not line.startswith(f"{i}. "):
                return TagType.PARAGRAPH
            i += 1
        return TagType.OLIST
    return TagType.PARAGRAPH



def mk_doc_to_html_node(markdown_doc: str) -> HTMLNode:
    markdown_blocks = markdown_to_blocks(markdown_doc)
    block_dict: dict[str, str] = {}
    for block in markdown_blocks:
        block_dict[block] = block_to_block_type(block)
    block_html_nodes: list[HTMLNode] = []
    block_tuples: zip[tuple[str, str]] = zip(
        block_dict.keys(), block_dict.values()
    )
    for block, type in block_tuples:
        if type == TagType.HEADING:
            block_html_nodes.append(heading_block_to_html_node(block))
        if type == TagType.CODEBLOCK:
            block_html_nodes.append(code_block_to_html_node(block))
        if type == TagType.QUOTE:
            block_html_nodes.append(quote_block_to_html_node(block))
        if type == TagType.UNOLIST:
            block_html_nodes.append(unordered_list_to_html_node(block))
        if type == TagType.OLIST:
            block_html_nodes.append(ordered_list_block_to_html_node(block))
        if type == TagType.PARAGRAPH:
            block_html_nodes.append(paragraph_block_to_html_node(block))
    return ParentNode(tag=TagType.DOC, children=block_html_nodes)

# //? Maybe done
def heading_block_to_html_node(heading_block: str) -> HTMLNode:
    stripped: str = heading_block[heading_block[0:6].count("#") + 1 :]
    new_node = ParentNode(tag=f"h{heading_block[0:6].count("#")}", children=[])
    delimiter_nester_in_node(new_node, stripped)

    return new_node

# //? Maybe done
def code_block_to_html_node(code_block: str) -> HTMLNode:
    children: list[HTMLNode] = []
    stripped: str = code_block.strip("```")
    stripped2 = stripped.lstrip("\n\r\x1d\x1e\u2028\u2029")
    children.append(LeafNode(text=stripped2))
    if bool(children) is False:
        assert "inline children is empty"
    return ParentNode(TagType.CODEBLOCK, children=children)

# //? Maybe done
def quote_block_to_html_node(quote_block: str) -> HTMLNode:
    children: list[HTMLNode] = []
    stripped: str = quote_block.replace(">", "\n")
    children.append(LeafNode(stripped))
    return ParentNode(tag=TagType.QUOTE, children=children)

# //? Maybe done
def unordered_list_to_html_node(unordered_list_block: str) -> HTMLNode:
    inline_children: list[HTMLNode] = []
    for line in unordered_list_block.split("\n"):
        if (
            line[0] == "-" or line[0] == "*" or line[0] == "+" or line[0] == "-"
        ) and set([item[0:2] for item in unordered_list_block.split("\n")]) <= set(
            ["- ", "* ", "+ ", "- "]
        ):
            new_node = ParentNode(TagType.LIST, children=[])
            inline_children.append(delimiter_nester_in_node(new_node, line[2:]))
            if bool(inline_children) is False:
                assert "inline children is empty"
    return ParentNode(tag=TagType.UNOLIST, children=inline_children)

# //? Maybe done
def ordered_list_block_to_html_node(ordered_list_block: str) -> HTMLNode:
    inline_children: list[HTMLNode] = []
    for line in ordered_list_block.split("\n"):
        new_node = ParentNode(TagType.LIST, children=[])
        inline_children.append(delimiter_nester_in_node(new_node, line[2:]))
        if bool(inline_children) is False:
            assert "inline children is empty"
    return ParentNode(tag=TagType.OLIST, children=inline_children)

# //? Maybe done
def paragraph_block_to_html_node(paragraph_block: str) -> HTMLNode:
    children: list[HTMLNode] = []
    new_node = ParentNode("p", children=children)
    delimiter_nester_in_node(new_node, paragraph_block)
    return new_node

def test() -> None:
    with open("./static_site/src/markdown_test") as file:
        # print(f"\n\n{[mk_doc_to_html_node(file.read())]}\n")
        html = mk_doc_to_html_node(file.read())
        realHtml = html.to_html()
        print(realHtml)
    return 

test()


# node = ParentNode(TagType.DOC, children=[])
# text = "This is ![image](https://storage.googleapis.com/qvault-webapp-dynamic-assets/course_assets/zjjcJKZ.png) an *italic-~~containing~~ sentence, **and also has an ![image](https://storage.googleapis.com/qvault-webapp-dynamic-assets/course_assets/zjjcJKZ.png) and a [link](https://boot.dev)** in bold*"
# # text = "This is **text** with an *italic* word and an with `some code *and fake* nest` with some extra"
# # text = "**and also has an ![image](https://storage.googleapis.com/qvault-webapp-dynamic-assets/course_assets/zjjcJKZ.png) and a [link](https://boot.dev)** in bold*"

# print(delimiter_nester_in_node(node, text))

# def test(node: ParentNode, text: str):

#     delimited = delimiter_nester_in_node(node, text)
#     html = delimited.to_html()
#     print(f"{html}")

# test(node, text)
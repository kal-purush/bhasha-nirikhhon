def qa(response):
    for answers in response.json()["data"]["summaryInsights"][0]["data"]["answers"][0]["document"]:
        print("\n", "ID:", answers["id"], "\n", "Title:", answers["title"], "\n", "Text:", answers["abs"], "\n", "Source:", answers["source"], "\n", "Publisher:", answers["index_name"])


def documents(response):
    for answers in response.json()["data"]["summaryInsights"][0]["data"]["answers"][0]["document"]:
        print("\n", "ID:", answers["id"], "\n", "Title:", answers["title"], "\n", "Text:", answers["abs"], "\n", "Source:", answers["source"], "\n", "Publisher:", answers["index_name"])


def search_graph(response):
    print(response.json()["data"]["graphInsights"]["data"]["description"], "\n", "Name:", response.json()["data"]["graphInsights"]["data"]["rootNode"]["name"])
    for answers in response.json()["data"]["graphInsights"]["data"]["nodes"]:
        print("\n", "ID:", answers["id"], "\n", "Name:", answers["name"], "\n", "Type:", answers["type"])


def search_text(response):
    for answers in response.json()["data"]["searchAsYouType"]["data"]["searchLabels"]:
        print("\n" "ID:", answers["qid"], "\n", "Text:", answers["text"], "\n", "NodeType:", answers["nodeTypes"][0])
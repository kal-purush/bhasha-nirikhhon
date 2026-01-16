from collections import defaultdict


def group_anagrams(collection: list[str]) -> list[list[str]]:
    """Given a list of anagrams group them"""
    group = defaultdict(list)

    for word in collection:
        group["".join(sorted(word))].append(word)

    result = list(group.values())

    return result
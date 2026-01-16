import re
from collections import namedtuple

Parsed = namedtuple("Parsed", "title, tags, content")


def parse_article(io, *, rx=re.compile('\[[^\]]+?\]')):
    tags = []
    title = ""
    itr = iter(io)
    for line in itr:
        if line.lstrip().startswith("#") and not line.lstrip().startswith("##"):
            title = line
            for m in rx.finditer(line.strip()):
                title = line[m.end():]
                tags.append(m.group(0).strip("[ ]"))
            break
    content = "".join(itr)
    return Parsed(title=title.strip(), tags=tags, content=content.strip())
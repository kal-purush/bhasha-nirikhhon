import os.path
import json
import logging
from .langhelpers import reify
logger = logging.getLogger(__name__)


class Data:
    def __init__(self, data):
        self.mapping = {d["id"]: d for d in data}
        self.order = [d["id"] for d in data]

    def append(self, post):
        self.order.append(post["id"])
        self.mapping[post["id"]] = post

    def __bool__(self):
        return len(self.mapping) > 0

    def __iter__(self):
        seen = set()
        for k in self.order:
            if k in seen:
                continue
            seen.add(k)
            yield self.mapping[k]


class Loader:
    def __init__(self, path, factory=Data):
        self.path = path
        self.factory = factory

    @reify
    def data(self):
        try:
            logger.info("read: %s", self.path)
            with open(self.path) as rf:
                return self.factory(json.load(rf))
        except FileNotFoundError:
            return []

    @reify
    def abspath_map(self):
        dirpath = os.path.abspath(os.path.dirname(self.path))
        return {os.path.normpath(os.path.join(dirpath, d["file"])): d["id"] for d in self.data}

    def lookup_id(self, path):
        abspath = os.path.normpath(os.path.abspath(path))
        return self.abspath_map.get(abspath)


class Saver:
    def __init__(self, path, data, *, docdir=None):
        self.path = path
        self.data = data
        self.docdir = docdir or os.path.join(os.path.dirname(self.path), "docs")

    def __enter__(self):
        return self.append

    def append(self, post, *, writefile=None, _retry=False):
        title = post["title"]
        if writefile is None:
            filepath = os.path.join(self.docdir, f"{post['id']}.md")
        else:
            filepath = writefile

        if writefile is None:
            try:
                logger.info("write: %s", filepath)
                with open(filepath, "w") as wf:
                    tagspart = "".join([f"[{t}]" for t in post["tags"]])
                    wf.write(f"#{tagspart}{title}\n")
                    wf.write(post["body"])
            except Exception:
                if _retry:
                    raise
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                return self.append(post, writefile=writefile, _retry=True)

        for k in ["tags", "body", "user", "comments"]:
            post.pop(k, None)
        relpath = os.path.relpath(filepath, start=os.path.dirname(self.path))
        post["file"] = relpath
        self.data.append(post)

    def __exit__(self, typ, val, tb):
        if not self.data:
            return
        with open(self.path, "w") as wf:
            logger.info("write: %s", self.path)
            json.dump(list(self.data), wf, indent=2, ensure_ascii=False)
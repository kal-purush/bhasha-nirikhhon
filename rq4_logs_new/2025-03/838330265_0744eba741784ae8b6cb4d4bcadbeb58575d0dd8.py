def get_link(text: str) -> str | None:
    try:
        link = text.split(" ")[1]
        return link
    except Exception:
        return None
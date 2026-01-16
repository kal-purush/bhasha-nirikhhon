def greet(name: str = "You", shout: bool = True) -> str:
    """
    Returns a greeting message with the given name.

    Args:
        name (str): The name to include in the greeting message. Defaults to "You".
        shout (bool): Whether to shout the greeting message. Defaults to True.

    Returns:
        str: The greeting message.
    """
    punctuation = "!" if shout else ""
    return "Hello" + " " + name + punctuation
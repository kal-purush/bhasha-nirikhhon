import errors

def smallint(arg):
    try:
        arg = int(arg)
    except ValueError:
        raise errors.BadDataError(f"{arg} is not an integer")
    
    if arg >= -32768 and arg <= 32767:
        return arg
    else:
        raise errors.BadDataError(f"{arg} is not in the range -32768 to 32767")

def idstr(arg):
    arg = str(arg)

    if not arg.isidentifier():
        raise errors.BadDataError("Pilgrimage ID must contain only alphanumeric characters and underscores, and cannot start with a number.")
    elif len(arg) > 63:
        raise errors.BadDataError("Pilgrimage IDs cannot be over 63 characters in length")
    else:
        return arg

def str127(arg):
    arg = str(arg)

    if len(arg) > 127:
        raise errors.BadDataError("Strings cannot be over 127 characters in length")
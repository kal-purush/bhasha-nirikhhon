"""
NIST Request/Response Utils
"""
import json, urlparse

class NISTError(Exception):
    def __init__(self, message, status):
        self.message = message
        self.status = status
        super(Exception, self).__init__(message)

def unpack(*arguments):
    """
    Unpack arguments to be used in methods wrapped
    """
    def decorator(func):
        def wrapper(_self, data, **kwargs):
            data = smart_parse(data)
            try:
                args = [data[item] for item in arguments]
            except KeyError:
                raise NISTError("%s was not provided in data" % item, 400)

            kwargs["_arguments"] = arguments

            func(_self, *args, **kwargs)
        return wrapper
    return decorator

def type_check(*types):
    """
    Checks unpacked arguments for types
    """
    types = map(lambda x: (str, unicode) if x is str else x, types)
    def decorator(func):
        def wrapper(_self, *args, **kwargs):
            for arg, _type, _arg in zip(args, types, kwargs.pop("_arguments")):
                if not isinstance(arg, _type):
                    raise NISTError(
                        "%s should be of type %s but '%s' is of type %s" %
                            (_arg, str(_type),  arg, str(type(arg)))
                    )
            func(_self, *args, **kwargs)
        return wrapper
    return decorator

def form_urlencoded_parse(body):
    """
    Parse x-www-form-url encoded data
    """
    try:
        data = urlparse.parse_qs(body, strict_parsing=True)
        for key in data:
            data[key] = data[key][0]
        return data
    except ValueError:
        raise NISTError("Invalid data input format", 400)

def smart_parse(body):
    """
    Handle json, fall back to x-www-form-urlencoded
    """
    try:
        data_dict = json.loads(body)
    except ValueError:
        return form_urlencoded_parse(body)
    return data_dict
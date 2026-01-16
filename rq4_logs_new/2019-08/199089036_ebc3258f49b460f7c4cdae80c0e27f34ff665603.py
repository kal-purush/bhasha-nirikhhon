#pylint: disable=missing-docstring

from cmdparserkhv import CmdParser, Cmdent

OPT_DIC = {
    "-obool": Cmdent("option_boolean"),
    "-onum": Cmdent("option_numeric", range(0, 10)),
    "-ostr": Cmdent("option_string", ["choice1", "choice2"])
}

def generic(use_case, wanted_result):
    """Generic test function"""
    cmd = CmdParser(use_case, OPT_DIC)
    actual_result = {
        "opt_dic_parsed": cmd.get_all_options(),
        "unrecognised": cmd.get_unrecognised()
    }
    assert actual_result == wanted_result

def test_empty():
    """Tests using the compiler with no options supplied"""
    use_case = ["script.py", "path/to/file"]
    wanted_result = {
        "opt_dic_parsed": {
            "option_boolean": False,
            "option_numeric": 0,
            "option_string": "choice1"
        },
        "unrecognised": ["path/to/file"]
    }
    generic(use_case, wanted_result)

def test_all_supplied():
    """Tests using the compiler with all options supplied"""
    use_case = [
        "script.py",
        "-obool", "-onum", "3", "-ostr", "choice2",
        "path/to/file"
    ]
    wanted_result = {
        "opt_dic_parsed": {
            "option_boolean": True,
            "option_numeric": 3,
            "option_string": "choice2"
        },
        "unrecognised": ["path/to/file"]
    }
    generic(use_case, wanted_result)
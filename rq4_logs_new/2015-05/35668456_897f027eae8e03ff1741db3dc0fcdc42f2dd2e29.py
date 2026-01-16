#!/usr/bin/env python2

import argparse
import sys
import logging
from pygccxml import parser
from pygccxml.declarations import matcher
from pygccxml.utils import loggers
loggers.cxx_parser.setLevel(logging.ERROR)


class Parser(object):
    def __init__(self, filename):
        self.filename = filename
        self.ns = parser.parse([filename], parser.gccxml_configuration_t())[0]

    def _remove_ns(self, string):
        return string.strip().lstrip("::")

    def _prettify(self, string):
        return string.strip().lstrip("__")

    def _no_restrict(self, string):
        return string.replace("__restrict__", "")

    def parse_functions(self, func_names):
        for func_name in func_names:
            try:
                func = self.ns.free_function(func_name)
            except matcher.declaration_not_found_t:
                continue
            yield {
                "name": self._remove_ns(func.name),
                "return_type": self._remove_ns(func.return_type.decl_string),
                "header": func.location.file_name,
                "args": [
                    {"name": self._prettify(arg.name), "type": self._prettify(self._remove_ns(self._no_restrict(arg.type.decl_string)))}
                    for arg in func.required_args
                ]
            }


class CodeGenerator(object):
    def __init__(self):
        self.functions = []

    def add_function(self, function):
        self.functions.append(function)

    def generate(self):
        raise NotImplementedError


class CCodeGenerator(CodeGenerator):
    TYPEDEF_TPL = "typedef {func[return_type]} (*orig_{func[name]}_f_type)({args});"
    IMPL_TPL = """{func[return_type]} {func[name]}({args})
{{
    orig_{func[name]}_f_type orig_{func[name]} = (orig_{func[name]}_f_type) dlsym(RTLD_NEXT, "{func[name]}");

    return orig_{func[name]}({arg_names});
}}"""
    IMPL_VARIADIC_TPL = """{func[return_type]} {func[name]}({args})
{{
    va_list args;
    va_start(args, {last_arg});

    {func[return_type]} ret = v{func[name]}({arg_names}, args);

    va_end(args);

    return ret;
}}"""
    INCLUDE_TPL = "#include <{filename}>"
    RESULT_TPL = """#define _GNU_SOURCE
#include <dlfcn.h>
{includes}

{typedefs}

{impls}"""


    def generate_decl(self, function):
        pass

    def generate(self):
        typedefs = []
        impls = []
        includes = []
        has_variadic = False
        for func in self.functions:
            variadic = "..." in (arg["type"] for arg in func["args"])
            args = ", ".join("%s %s" % (arg["type"], arg["name"]) for arg in func["args"])
            if variadic:
                has_variadic = True
                last_arg = func["args"][-2]["name"]
                arg_names = ", ".join(arg["name"] for arg in func["args"][:-1])
                impl = self.IMPL_VARIADIC_TPL.format(func=func, args=args, arg_names=arg_names, last_arg=last_arg)
            else:
                arg_names = ", ".join([arg["name"] for arg in func["args"]])
                typedef = self.TYPEDEF_TPL.format(func=func, args=args)
                typedefs.append(typedef)
                impl = self.IMPL_TPL.format(func=func, args=args, arg_names=arg_names)
            impls.append(impl)
            include = self.INCLUDE_TPL.format(filename=func["header"])
            if include not in includes:
                includes.append(include)
        return self.RESULT_TPL.format(includes="\n".join(includes), typedefs="\n".join(typedefs), impls="\n\n".join(impls))


class RustCodeGenerator(CodeGenerator):
    pass



def main(filename, funcs):
    parser = Parser(filename)
    generator = CCodeGenerator()
    for func in parser.parse_functions(funcs):
        generator.add_function(func)
    print generator.generate()


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("filename")
    argparser.add_argument("func", nargs="+")
    args = argparser.parse_args()
    main(args.filename, args.func)
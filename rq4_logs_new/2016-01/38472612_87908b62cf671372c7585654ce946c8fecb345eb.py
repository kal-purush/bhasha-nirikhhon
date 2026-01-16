################################################################################
# Filename: util/subparsers/package.py
# Author:   Brandon Milton, http://brandonio21.com
# Date:     05 January 2016
#
# Contains logic for the subparser that is invoked when calling
# $ ./runner.py package
################################################################################

SUBPARSER_KEYWORD = "package"

def operate(args):
    """
    Takes the passed in args and delegates to the proper functionality. This is
    set as the executable function when the `package` subparser is used

    Arguments:
    args: Namespace - The arguments passed in via CLI
    """
    pathList = args.paths
    package(pathList)

def add_to_subparser_object(subparserObject, parentParser):
    """
    Adds the package subparser to a given subparsers object and delegates 
    package functionality to the operate() function

    Arguments:
    subparserObject - The ArgumentParser given by parser.add_subparsers() to add
                      the test subparser to
    parentParser    - The parser to be included as a parent to the subparser,
                      useful for global flags
    """
    packageParser = subparserObject.add_parser(SUBPARSER_KEYWORD, parents=[parentParser])
    packageParser.add_argument('paths', nargs='+')
    packageParser.add_argument('--config')
    packageParser.set_defaults(func=operate)
"""ARGS

:Description: Handling of command line arguments.

:Author: Martin Kilbinger <martin.kilbinger@cea.fr>


"""


def parse_options(p_def, short_options, types, help_strings):
    """Parse command line options.

    Parameters
    ----------
    p_def : dict
        default parameter values
    short_options : dict
        command line options short (one character) versions
    types : dict
        command line options types
    help_strings : dict
        command line options help strings

    Returns
    -------
    options: tuple
        Command line options
    """

    usage  = "%prog [OPTIONS]"
    parser = OptionParser(usage=usage)

    # Loop over default parameter values
    for key in p_def:

        # Process if help string exists
        if key in help_strings:

            # Set short option
            if key in short_options:
                short = short_options[key]
            else:
                # Default is no short option
                short = ''

            # Set type
            if key in types:
                typ = types[key]
            else:
                # Default is str
                typ = 'string'

            # Special case: bool type
            if typ == 'bool':
                parser.add_option(
                    f'{short}',
                    f'--{key}',
                    dest=key,
                    default=False,
                    action='store_true',
                    help=help_strings[key].format(p_def[key]),
                )
            else:
                parser.add_option(
                    short,
                    f'--{key}',
                    dest=key,
                    type=typ,
                    default=p_def[key],
                    help=help_strings[key].format(p_def[key]),
                )

    # Add verbose option
    parser.add_option(
        '-v',
        '--verbose',
        dest='verbose',
        action='store_true',
        help=f'verbose output'
    )

    options, args = parser.parse_args()

    return options
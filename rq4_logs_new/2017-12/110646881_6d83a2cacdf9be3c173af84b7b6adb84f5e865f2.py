"""
To generate quick enum with name support
"""


def enum(*sequential, **named):
    """
    Automatic enumeration with support for converting
    values back into names
    """
    enums = dict(((x, i) for i, x in enumerate(sequential)), **named)
    reverse = dict((value, key) for key, value in enums.iteritems())
    enums['get_name'] = reverse
    return type('Enum', (), enums)


def main():
    """
    Running the code
    """
    numbers = enum("ZERO", ONE=1, TWO=2, THREE='three')

    # pylint: disable=no-member
    print numbers.ZERO
    print numbers.ONE
    print numbers.THREE
    print numbers.get_name["three"]

if __name__ == "__main__":
    main()
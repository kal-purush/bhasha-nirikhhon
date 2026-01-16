"""pytest-redis queue plugin implementation."""

import redis

__version__ = "0.2.4"


def pytest_addoption(parser):
    """Add command line options to py.test command."""
    parser.addoption('--redis-host', metavar='redis_host',
                     type=str, help='The host of the redis instance.',
                     required=True)
    parser.addoption('--redis-port', metavar='redis_port',
                     type=str, help='The port of the redis instance.',
                     required=True)
    parser.addoption('--redis-pop-type', metavar='redis_pop_type',
                     type=str,
                     help=('The key of the redis list containing '
                           'the test paths to execute.'),
                     default="RPOP")
    parser.addoption('--redis-list-key', metavar='redis_list_key',
                     type=str,
                     help=('The key of the redis list containing '
                           'the test paths to execute.'),
                     required=True)


def retrieve_test_from_redis(redis_connection, list_key, command):
    """Remove and return a test path from the redis queue."""
    val = None
    if command.lower() == "lpop":
        val = redis_connection.lpop(list_key)
    elif command.lower() == "rpop":
        val = redis_connection.rpop(list_key)
    return val


def pytest_cmdline_preparse(config, args):
    """
    Called before cmd line options are parsed.

    We jump the gun and extract redis_host, redist_port
    and redis_list_key from the cmdline args to start
    pulling tests off the queue and add the tests to
    the cmd line.
    """
    parse_args = config._parser.parse_known_args(args)

    r_client = redis.StrictRedis(host=parse_args.redis_host,
                                 port=parse_args.redis_port)

    val = retrieve_test_from_redis(r_client,
                                   parse_args.redis_list_key,
                                   parse_args.redis_pop_type)
    if val is None:
        print "No items in redis queue '%s'" % parse_args.redis_list_key
        print "Running all tests"

    while val is not None:
        args.append(val)
        val = retrieve_test_from_redis(r_client,
                                       parse_args.redis_list_key,
                                       parse_args.redis_pop_type)
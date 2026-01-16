#!/usr/bin/env python3
from wifiology_node_poc.webapp.application import webapp_argument_parser, create_webapp, \
    webapp_argparse_args_to_kwargs

webapp_argument_parser.add_argument(
    "-b", "--bind-host", type=str, default="127.0.0.1", help="The host address to bind to."
)
webapp_argument_parser.add_argument(
    "-p", "--bind-port", type=int, default=9002, help="The port to listen on."
)

if __name__ == "__main__":
    args = webapp_argument_parser.parse_args()
    app = create_webapp(**webapp_argparse_args_to_kwargs(args))
    app.run(
        server='eventlet', host=args.bind_host, port=args.bind_port
    )
#! /usr/bin/env python

#
#    Modular REST API dispatcher in Python (dispytch)
#
#    Copyright (C) 2015 Denis Pompilio (jawa) <denis.pompilio@gmail.com>
#    Copyright (C) 2015 Cyrielle Camanes (cycy) <cyrielle.camanes@gmail.com>
#
#    This file is part of dispytch
#
#    This program is free software; you can redistribute it and/or
#    modify it under the terms of the GNU General Public License
#    as published by the Free Software Foundation; either version 2
#    of the License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program; if not, see <http://www.gnu.org/licenses/>.


import os
import sys
import docopt
import json


def output_json(data):
    """Output passed datas as json

    :arg datas: Data to output as json, type must be supported by json format
    """
    print(json.dumps(data, indent=2))


def parse_documentpath(string):
    """Parse URL document path and return datas dict

    :arg str string: Document path REST call
    :return: Positionnal arguments from parsed document path
    :rtype: list
    """
    fields = ["method", "target", "datatype", "cf", "start", "stop", "tmpl"]
    values = string.split("/")[2:]
    if len(values) == 6:
        values.append(None)
    return dict(zip(fields, values))


def parse_urlencoded(string):
    """Parse URL-Encoded format and return datas dict

    :arg str string: URL-Encoded string
    :return: Named arguments from parsed URL-Encoded string
    :rtype: dict
    """
    datas = {}
    for field in string.split("&"):
        if "=" in field:
            key, val = field.split("=", 1)
            datas.update({key: val})
    return datas


def receive_request():
    """Receive request from GET, POST or cli methods

    :return: Tuple of parsed positionnal and named arguments
    :rtype: tuple
    """
    method = os.environ.get('REQUEST_METHOD')
    datas = ([], {})

    print("method: {0}".format(method))
    if method is None:
        # request comes from cli
        pass

    elif method == "POST":
        # read url-encoded input from stdin
        print("type: urlencoded request")
        datas[1].update(parse_urlencoded(sys.stdin.read()))

    elif method == "GET":
        # retreive REQUEST_URI from environment
        req_uri = os.environ.get('REQUEST_URI', '')
        print("requri: {0}".format(req_uri))
        if "?" in req_uri:
            print("type: urlencoded request")
            datas[1].update(parse_urlencoded(req_uri.split("?", 1)[-1]))
        else:
            print("type: documentpath request")
            datas[0].extend(parse_documentpath(req_uri))

    return datas


if __name__ == "__main__":
    print "Content-type: text/plain"
    print ""

    try:
        print("start")
        request = receive_request()
        print("received")
    except Exception as e:
        print(e)

    print("request: {0}".format(request))
    if len(request):
        try:
            dispatch_request(request)
        except:
            import traceback
            info = sys.exc_info()
            _dump({'error': info[1].message, })
            print('\n# '.join(__doc__.split("\n")))

            # For debug purposes
            print ''.join(traceback.format_exception(*info))
            exit(1)
    else:
        print(__doc__)
        exit(1)
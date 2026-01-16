#!/usr/bin/python

try:
    import simplejson as json
except ImportError:
    import json
import urllib2
import argparse
import StringIO
import sys
from textwrap import wrap, fill


def run(wf_url, label_unnamed_data, nodes_are_analyses):
    contents = urllib2.urlopen(wf_url).read()
    # Convert workflow string into json object
    contents = contents.replace('\"', '"')
    # print contents

    wf = json.loads(contents)
    results = convert(wf, label_unnamed_data, nodes_are_analyses)
    return results


def convert(workflow, label_unnamed_data, nodes_are_analyses):
    if label_unnamed_data and not nodes_are_analyses:
        sys.stderr.write('Warning: --label_unnamed_data does nothing if graph nodes are datasets\n')
    if workflow["format-version"] != "0.1":
        sys.stderr.write("Unknown format version {0}, continuing anyway\n".format(workflow['format-version']))

    # Get step objects from galaxy workflow json
    steps = workflow['steps']
    input_count = 0
    nodes_dict = []
    # Create data and tool nodes
    for step in sorted(steps, key=int):
        # Input data node
        if steps[step]['name'] == 'Input dataset':
            ts_content = steps[step]['tool_state']
            tool_state = json.loads(ts_content)

            # for key, value in tool_state.iteritems():
            #     print '"' + key + '":"' + value + '"'

            if tool_state is None:
                tool_state = "None"
            node_data = {'id': 'n' + str(input_count), "name": "Input dataset", "color": "#FCF8E3", "tool_state": tool_state}
            nodes_dict.append({"data": node_data})
        else:
            name = steps[step]['name']
            tool_version = steps[step]['tool_version']
            if tool_version is None:
                tool_version = "None"

            ts_content = steps[step]['tool_state']
            tool_state = json.loads(ts_content)
            if tool_state is None:
                tool_state = "None"

            tool_id = steps[step]['tool_id']
            if tool_id is None:
                tool_id = "None"

            type = steps[step]['type']
            if type is None:
                type = "None"
            node_tool = {'id': 'n' + str(input_count), "name": name, "color": "#D9EDF7", "tool_version": tool_version, "tool_state": tool_state, "tool_id": tool_id, "type": type}
            nodes_dict.append({"data": node_tool})
        input_count += 1

    # Create edges
    input_count = 0
    edges_dict = []
    for step in sorted(steps, key=int):
        if steps[step]['name'] == 'Input dataset':
            pass
        else:
            for input_name in steps[step]['input_connections']:
                start_step_num = unicode(steps[step]['input_connections'][input_name]['id'])

                edge_data = {'id': 'e' + str(input_count), 'weight': '1', 'source': 'n' + start_step_num, 'target': 'n' + step}
                edges_dict.append({"data": edge_data})
                input_count += 1

    content = {'nodes': nodes_dict, 'edges': edges_dict}
    elements_dict = {"elements": content}
    json_data = json.dumps(elements_dict, separators=(',', ':'), indent=2)
    return json_data


def main():
    parser = argparse.ArgumentParser(description="Convert Galaxy workflow JSON to cytoscape JSON")
    # Positional argument
    parser.add_argument("workflow_file",
                        type=file,
                        help="Galaxy workflow description (JSON format)")

    # Optional arguments
    parser.add_argument('--label_unnamed_data',
                        action='store_true',
                        default=False,
                        help="Label datasets even if they have no explicit name (rename dataset action)")
    parser.add_argument('--nodes_are_analyses',
                        action='store_true',
                        default=False,
                        help="Graph nodes are analyses (by default they are datasets)")
    parser.add_argument('--no_graph_name',
                        action='store_true',
                        default=False,
                        help="Omit the name of the graph")
    parser.add_argument('--output_filename',
                        help="Output filename (default is to print to stdout)")
    args = parser.parse_args()

    global output
    if args.output_filename:
        output = open(args.output_filename, 'w')
    else:
        output = sys.stdout

    # Convert workflow file into json object
    wf = json.load(args.workflow_file)
    # Convert json to cytoscape format
    convert(wf, args.label_unnamed_data, args.nodes_are_analyses)

if __name__ == "__main__":
    main()
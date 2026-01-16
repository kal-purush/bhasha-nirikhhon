def map_to_avro(graph):
    """Convert NetworkX graph to Avro records."""
    node_records = []
    edge_records = []
    
    
    for node_id, attrs in graph.nodes(data=True):
        node_records.append({
            "id": str(node_id),
            "label": attrs.get("label", None),
            "properties": {k: str(v) for k, v in attrs.items() if k != "label"}
        })
    
    
    for src, tgt, attrs in graph.edges(data=True):
        edge_records.append({
            "source": str(src),
            "target": str(tgt),
            "label": attrs.get("label", None),
            "properties": {k: str(v) for k, v in attrs.items() if k != "label"}
        })
    
    return node_records, edge_records

import fastavro

def write_avro(records, schema_path, output_path):
    """Serialize records to Avro with schema validation."""
    # Load schema
    with open(schema_path, 'r') as f:
        schema = fastavro.parse_schema(json.load(f))
    
    # Validate and write
    with open(output_path, 'wb') as out:
        fastavro.writer(out, schema, records)

def graph_to_pfb(graph_path, output_dir, schema_dir):
    # Parse graph
    G = parse_edgelist(graph_path)
    
    # Map to Avro records
    nodes, edges = map_to_avro(G)
    
    # Write nodes and edges to separate files
    write_avro(nodes, f"{schema_dir}/node_schema.avsc", f"{output_dir}/nodes.avro")
    write_avro(edges, f"{schema_dir}/edge_schema.avsc", f"{output_dir}/edges.avro")




    def validate_avro(file_path, schema_path):
    """Check if Avro file adheres to schema."""
    with open(file_path, 'rb') as f:
        reader = fastavro.reader(f)
        assert reader.schema == fastavro.parse_schema(json.load(open(schema_path)))



graph_to_pfb(
    graph_path="data/graph.edgelist",
    output_dir="output",
    schema_dir="schemas"
)


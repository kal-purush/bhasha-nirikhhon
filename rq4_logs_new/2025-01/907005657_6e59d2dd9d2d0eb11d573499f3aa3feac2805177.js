// Initialize Vis.js graph
const nodes = new vis.DataSet([
    { id: 'A', label: 'A' },
    { id: 'B', label: 'B' },
    { id: 'C', label: 'C' },
    { id: 'D', label: 'D' },
    { id: 'E', label: 'E' },
    { id: 'F', label: 'F' },
]);

const edges = new vis.DataSet([
    { from: 'A', to: 'B', label: '1' },
    { from: 'A', to: 'C', label: '4' },
    { from: 'B', to: 'D', label: '2' },
    { from: 'B', to: 'E', label: '5' },
    { from: 'C', to: 'F', label: '3' },
    { from: 'E', to: 'F', label: '1' },
]);

const container = document.getElementById('graph-container');
const data = { nodes, edges };
const options = {
    physics: {
        enabled: true,
    },
};
const network = new vis.Network(container, data, options);


// Highlight nodes and edges
function highlightPath(path, algo) {
    const highlightedNodes = path.map((node) => ({ id: node, color: '#ffcc00' }));
    const highlightedEdges = [];

    for (let i = 0; i < path.length - 1; i++) {
        const fromNode = path[i];
        const toNode = path[i + 1];

        // For BFS and DFS, hide the cost label; for UCS, show it
        const edgeLabel = (algo === 'ucs') ? graph[fromNode][toNode] : '';

        highlightedEdges.push({
            from: fromNode,
            to: toNode,
            color: { color: '#ffcc00' },
            label: edgeLabel,
        });
    }

    // Update nodes and edges
    nodes.update(highlightedNodes);
    edges.update(highlightedEdges);
}

// Reset graph to original state
function resetGraph() {
    nodes.update(nodes.get().map((node) => ({ id: node.id, color: '#97C2FC' }))); // Default color
    edges.update(edges.get().map((edge) => ({ ...edge, color: { color: '#848484' }, label: '' }))); // Default style
}


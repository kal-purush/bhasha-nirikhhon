const p5_graphVisualization = new p5(p => {
    let graph;
    let lastNode;
    let i = 1;

    let draggedNode = null;
    let isDragging = false;
    let connectorNode = null;
    let isConnecting = false;
    let nodesVisible = true;
    let edgesVisible = true;
    let hasGravity = true;
    let isDirected = true;
    let showHelp = true;
    let isDepthFirstTraversing = false;
    let isBreadthFirstTraversing = false;

    const keys = {
        after: {
            dir: 'a',
            und: 'z',
        },
        random: {
            dir: 's',
            und: 'x',
        },
        connect: {
            dir: 'd',
            und: 'c',
        },
        toggle: {
            nodeG: 'q',
            nodeVis: 'w',
            edgeVis: 'e',
            dirVis: 'r',
            help: 'h',
        },
        traverse: {
            depth: 'f',
            breadth: 'g',
            end: 'v',
        },
    }
    const stateOf = b => b ? 'on' : 'off';
    let traversalStack;

    let traversalIterator = null;

    p.setup = () => {
        p.createCanvas(700, 700);
        p.background(0, 255, 0);

        lastNode = new NodeDot(p.width / 2, p.height / 2, 0);

        graph = new Graph(lastNode.graphNode);

        traversalStack = new ContextManagedStack(
            item => {
                item.color = 'red';
            },
            item => {
                item.color = 'white';
            },
        );
    };

    p.draw = () => {
        p.background(0, 220, 0);

        if (showHelp) {
            p.push();
            const fontSize = 20;
            const message = [
                '[Movement]',
                'Click and drag over a node to move it',
                'Shift+click to link a new node after the previous node',
                'Ctrl+click to insert a new node at a random position',
                '',
                '[Adding nodes]',
                'A+click to add a directed node after the previous node',
                'Z+click to add an undirected node after the previous node',
                'S+click to add a directed node to a random node',
                'X+click to add an undirected node to a random node',
                'D+click to add a directed edge between two nodes',
                'C+click to add an undirected edge between two nodes',
                '',
                '[Traversal]',
                `F to step through a depth first traversal [${stateOf(isDepthFirstTraversing)}]`,
                `G to step through a breadth first traversal [${stateOf(isBreadthFirstTraversing)}]`,
                'V to cancel the traversal',
                '',
                '[Other]',
                `Q to toggle node gravity [${stateOf(hasGravity)}]`,
                `W to toggle node visibility [${stateOf(nodesVisible)}]`,
                `E to toggle edge visibility [${stateOf(edgesVisible)}]`,
                `R to toggle edge direction visibility [${stateOf(isDirected)}]`,
                `H to toggle this help message [${stateOf(showHelp)}]`,
            ];

            p.fill(0);
            p.textSize(fontSize);

            const msg = message.reduce((a, b) => a + '\n' + b, '')
            p.text(msg, 5, 0);
            p.pop();
        }

        if (!p.mouseIsPressed) {
            isDragging = false;
        }

        if (!p.keyIsPressed || !(p.key == keys.connect.dir || p.key == keys.connect.und)) {
            isConnecting = false;
        }

        // Draw the graph
        if (edgesVisible) {
            for (const nodeDot of graph.depthFirst()) {
                nodeDot.displayLines(isDirected);
            }
        }
        if (nodesVisible) {
            for (const nodeDot of graph.depthFirst()) {
                nodeDot.displayDot();
            }
        }

        const globalNodeDots = [...graph.depthFirst()];
        if (hasGravity) {
            for (const nodeDot of graph.breadthFirst()) {
                nodeDot.update(globalNodeDots, 0, 0, p.width, p.height);
            }
        }

        if (isDragging && draggedNode != null) {
            const dot = draggedNode;
            dot.x = p.mouseX;
            dot.y = p.mouseY;
        }
    };

    p.keyPressed = () => {
        if (p.key == keys.toggle.nodeG) {
            hasGravity = !hasGravity;
        } else if (p.key == keys.toggle.nodeVis) {
            nodesVisible = !nodesVisible;
        } else if (p.key == keys.toggle.edgeVis) {
            edgesVisible = !edgesVisible;
        } else if (p.key == keys.toggle.dirVis) {
            isDirected = !isDirected;
        } else if (p.key == keys.toggle.help) {
            showHelp = !showHelp;
        } else if (p.key == keys.traverse.depth) {
            advanceDepthFirst();
        } else if (p.key == keys.traverse.breadth) {
            advanceBreadthFirst();
        } else if (p.key == keys.traverse.end) {
            endTraversal();
        }
    };

    p.mousePressed = () => {
        if (p.keyIsPressed && (p.key == keys.connect.dir || p.key == keys.connect.und)) {
            if (!isConnecting) {
                isConnecting = true;
                connectorNode = nodeAtPosition(graph, p.mouseX, p.mouseY);

                if (connectorNode == null) {
                    isConnecting = false;
                }
            } else {
                const connectedNode = nodeAtPosition(graph, p.mouseX, p.mouseY);

                if (connectedNode != null) {
                    if (p.key == keys.connect.dir) {
                        connectorNode.add(connectedNode);
                    } else if (p.key == keys.connect.und) {
                        connectorNode.undirectedAdd(connectedNode);
                    }
                } else {
                    const newNode = new NodeDot(p.mouseX, p.mouseY, i++);

                    if (p.key == keys.connect.dir) {
                        connectorNode.add(newNode);
                    } else if (p.key == keys.connect.und) {
                        connectorNode.undirectedAdd(newNode);
                    }

                    lastNode = newNode;
                }
                isConnecting = false;
            }
        } else if (p.keyIsPressed) {
            const newNode = new NodeDot(p.mouseX, p.mouseY, i++);
            if (p.key == keys.after.dir || p.key == keys.after.und) {
                if (p.key == keys.after.dir) {
                    lastNode.add(newNode);
                } else {
                    lastNode.undirectedAdd(newNode);
                }
            } else if (p.key == keys.random.dir || p.key == keys.random.und) {
                if (p.key == keys.random.dir) {
                    randomNode(graph).add(newNode);
                } else {
                    randomNode(graph).undirectedAdd(newNode);
                }
            }
            lastNode = newNode;
        } else {
            if (!isDragging) {
                draggedNode = null;
                isDragging = true;
            }
            if (draggedNode == null) {
                draggedNode = nodeAtPosition(graph, p.mouseX, p.mouseY);
            }
        }
    }

    function endTraversal() {
        isDepthFirstTraversing = false;
        isBreadthFirstTraversing = false;

        while (!traversalStack.empty()) {
            traversalStack.pop();
        }
    }

    function advanceDepthFirst() {
        if (isBreadthFirstTraversing) {
            endTraversal();
        }

        if (!isDepthFirstTraversing) {
            traversalIterator = graph.depthFirst();
            isDepthFirstTraversing = true;
        }

        const {value, done} = traversalIterator.next();

        if (!done) {
            traversalStack.push(value);
        } else {
            endTraversal();
        }
    }

    function advanceBreadthFirst() {
        if (isDepthFirstTraversing) {
            endTraversal();
        }

        if (!isBreadthFirstTraversing) {
            traversalIterator = graph.breadthFirst();
            isBreadthFirstTraversing = true;
        }

        const {value, done} = traversalIterator.next();
        if (!done) {
            traversalStack.push(value);
        } else {
            endTraversal();
        }
    }

    function randomNode(graph) {
        const list = [...graph.depthFirst()];
        return list[Math.floor(Math.random() * list.length)];
    }

    function nodeAtPosition(graph, x, y) {
        return [...graph.depthFirst()]
            .find(nodeDot => {
                let {x: dotX, y: dotY, r} = nodeDot;
                return p.dist(dotX, dotY, x, y) < r + 5;
            });
    }

    class Queue {
        constructor() {
            this.list = [];
        }

        push(item) {
            this.list.push(item);
        }

        pop() {
            return this.list.shift();
        }

        empty() {
            return this.list.length == 0;
        }

        *[Symbol.iterator]() {
            for (let i = 0; i < this.list.length; ++i) {
                yield this.list[i];
            }
        }
    }

    class Stack {
        constructor() {
            this.list = [];
        }

        push(item) {
            this.list.push(item);
        }

        pop() {
            return this.list.pop();
        }

        empty() {
            return this.list.length == 0;
        }

        contains(item) {
            for (const value of this.list) {
                if (value == item) {
                    return true;
                }
            }
        }

        *[Symbol.iterator]() {
            for (let i = this.list.length - 1; i >= 0; --i) {
                yield this.list[i];
            }
        }
    }

    class GraphNode {
        constructor(data, ...nodes) {
            this.nodes = nodes || [];
            this.data = data;
        }

        getValue() {
            return this.data;
        }

        add(node) {
            if (node != null && !this.contains(node)) {
                this.nodes.push(node);
            }
        }

        addAll(nodes) {
            for (const node of this.nodes) {
                this.add(node);
            }
        }

        contains(node) {
            for (const n of this.nodes) {
                if (n == node) {
                    return true;
                }
            }
            return false;
        }

        *getNodes() {
            for (const node of this.nodes) {
                yield node;
            }
        }

        *getNodesReverse() {
            for (let i = this.nodes.length - 1; i >= 0; --i) {
                yield this.nodes[i];
            }
        }
    }

    class Graph {
        constructor(node) {
            this.startNode = node;
        }

        *depthFirstNodes() {
            const path = new Stack();
            const visited = new Stack();

            if (this.startNode != null) {
                path.push(this.startNode);
                visited.push(this.startNode);
            }

            while (!path.empty()) {
                const head = path.pop();

                yield head;

                for (const node of head.getNodesReverse()) {
                    if (!visited.contains(node)) {
                        path.push(node);
                        visited.push(node);
                    }
                }
            }
        }

        *depthFirst() {
            for (const node of this.depthFirstNodes()) {
                yield node.data;
            }
        }

        *breadthFirstNodes() {
            const path = new Queue();
            const visited = new Stack();

            if (this.startNode != null) {
                path.push(this.startNode);
                visited.push(this.startNode);
            }

            while (!path.empty()) {
                const head = path.pop();

                yield head;

                for (const node of head.getNodes()) {
                    if (!visited.contains(node)) {
                        path.push(node);
                        visited.push(node);
                    }
                }
            }
        }

        *breadthFirst() {
            for (const node of this.breadthFirstNodes()) {
                yield node.data;
            }
        }
    }

    class NodeDot {
        constructor(x, y, data, ...nodeDots) {
            this.x = x;
            this.y = y;
            this.vx = 0;
            this.vy = 0;
            this.r = 15;
            this.color = "white";
            this.lineColor = "black"
            this.textColor = "black";
            this.data = data;
            this.nodeDots = nodeDots;
            this.graphNode = new GraphNode(this, ...this.getGraphNodes());
        }

        *getGraphNodes() {
            for (const nodeDot of this.nodeDots) {
                yield nodeDog.graphNode;
            }
        }

        contains(node) {
            for (const n of this.nodeDots) {
                if (n == node) {
                    return true;
                }
            }
            return false;
        }

        add(node) {
            if (node != null && !this.contains(node)) {
                this.nodeDots.push(node);
            }
            this.graphNode.add(node.graphNode);
        }

        undirectedAdd(node) {
            this.add(node);
            node.add(this);
        }

        update(globalDots, left, top, width, height) {
            const speed = 20;
            const attractDist = 20 * this.r;
            const repelDist = 10 * this.r;
            const border = 0.25 * this.r;

            const impulses = globalDots
                .concat(this.nodeDots)
                .filter(dot => dot != this)
                .map(dot => {
                    let distSq = Math.pow(this.x - dot.x, 2) + Math.pow(this.y - dot.y, 2);
                    let dist = Math.sqrt(distSq);

                    if (dist <= 0.01) {
                        const dir = Math.random() * p.TWO_PI;
                        const r = 0.1;
                        return [r * Math.cos(dir), r * Math.sin(dir)];
                    }

                    let dx = (dot.x - this.x) * speed;
                    let dy = (dot.y - this.y) * speed;

                    dx /= -distSq;
                    dy /= -distSq;

                    if (attractDist < dist) {
                        dx *= -1;
                        dy *= -1;
                    } else if (repelDist < dist) {
                        dx *= 0;
                        dy *= 0;
                    }

                    return [dx, dy];
                })
                .reduce(([x, y], [dx, dy]) => [x + dx, y + dy], [0, 0]);

            let [dx, dy] = impulses;

            // Distance out of bounds in each direction
            const distXL = left + border - (this.x - this.r);
            const distXR = this.x + this.r - (left + width - border);
            const distYT = top + border - (this.y - this.r);
            const distYB = this.y + this.r - (top + height - border);

            // Resolve nodes out of bounds
            if (distXL > 0) {
                dx = distXL
            }
            if (distXR > 0) {
                dx = -distXR;
            }
            if (distYT > 0) {
                dy = distYT;
            }
            if (distYB > 0) {
                dy = -distYB;
            }

            const threshold = 0.1
            if (Math.abs(dx) < threshold) {
                dx = 0;
            }
            if (Math.abs(dy) < threshold) {
                dy = 0;
            }

            // Update velocities
            this.vx = dx;
            this.vy = dy;

            // Update positions
            this.x += this.vx;
            this.y += this.vy;
        }

        displayLine(isDirected, x1, y1, x2, y2) {
            p.line(x1, y1, x2, y2);

            const mag = p.dist(x1, y1, x2, y2);

            if (isDirected && mag != 0) {
                const size = 16;
                const arrowPos = 5 / 8;
                const dx = (x2 - x1) * size / mag;
                const dy = (y2 - y1) * size / mag;
                const midX = x1 * (1 - arrowPos) + x2 * arrowPos;
                const midY = y1 * (1 - arrowPos) + y2 * arrowPos;

                const s = 1 / 4;
                p.triangle(
                    midX + dx, midY + dy,
                    midX - dy * s, midY + dx * s,
                    midX + dy * s, midY - dx * s,
                );
            }
        }

        display() {
            this.displayLines(true);
            this.displayDot();
        }

        displayDot() {
            p.push();
            p.textSize(1.2 * this.r);
            p.fill(this.color);
            p.circle(this.x, this.y, this.r * 2);

            if (this.data != null) {
                p.fill(this.textColor);
                const s = `${this.data}`;
                const offset = -0.35 * s.length * this.r;

                p.text(this.data, this.x + offset, this.y + 0.5 * this.r);
            }
            p.pop();
        }

        displayLines(isDirected) {
            p.push();
            p.fill(this.lineColor);
            p.stroke(this.lineColor);
            for (const node of this.nodeDots) {
                p.line(this.x, this.y, node.x, node.y);
                this.displayLine(isDirected, this.x, this.y, node.x, node.y);
            }
            p.pop();
        }
    }

    class ContextManagedStack {
        constructor(onPush, onPop) {
            this.stack = new Stack();
            this.onPush = onPush;
            this.onPop = onPop;
        }

        push(item) {
            this.onPush(item);
            this.stack.push(item);
        }

        pop() {
            const item = this.stack.pop();
            this.onPop(item);
        }

        empty() {
            return this.stack.empty();
        }

        contains(item) {
            return this.stack.contains(item);
        }
    }
});
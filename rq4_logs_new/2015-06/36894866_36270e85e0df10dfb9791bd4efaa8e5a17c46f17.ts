module Tree {
    function argmax(collection, fun) {
        var element = collection[0];
        var max = fun(element);
    
        for (var i = 1; i < collection.length; i++) {
            var current = collection[i];
            var result = fun(current);
    
            if (result > max) {
                max = result;
                element = current;
            }
        }
    
        return element;
    }
    
    interface plainNodeObject { distance: number; children: Array<plainNodeObject>; }
    
    function toPlainObject(node) {
        var object = { distance: node.distance, children: [] };
    
        if (node.children && node.children.length === 2)
            object.children = node.children.map(toPlainObject);
    
        return object;
    }
    
    function fromPlainObject(object: plainNodeObject, parent: GraphNode) {
        var node = new GraphNode(object.distance, parent);
    
        if (object.children && object.children.length === 2) {
            node.addChildren.apply(node, object.children.map(function(childObject) {
                return fromPlainObject(childObject, node);
            }));
        }
    
        return node;
    }

    export class GraphNode {
        children: Array<GraphNode> = null;
        
        constructor(public distance : number, public parent : GraphNode = null) {}
    
        addChildren(left : GraphNode, right : GraphNode) : void {
            this.children = [left, right];
        }
    
        isRoot() : boolean {
            return this.parent === null;
        }
    
        getRoot() : GraphNode {
            if (this.isRoot()) return this;
    
            return this.parent.getRoot();
        }
    
        isLeafNode() : boolean {
            return this.children === null;
        }
    
        getDepth() : number {
            if (this.isRoot()) return this.distance;
    
            return this.distance + this.parent.getDepth();
        }
        
        getLevelDepth() : number {
            if (this.isRoot()) return 1;
    
            return 1 + this.parent.getLevelDepth();
        }
    
        getDeepestLeaf(distance : number = 0) {
            var accumulatedDistance = distance + this.distance;
            
            if (this.isLeafNode()) {
                return {
                    distance: accumulatedDistance,
                    node: this
                };
            }
    
            return argmax(this.children.map(function(child) {
                return child.getDeepestLeaf(accumulatedDistance);
            }), function(result) {
                return result.distance;
            });
        }
    
        
        getLevelDeepestLeaf(depth : number = 0) {
            var accumulatedDepth = depth + 1;
            
            if (this.isLeafNode()) {
                return {
                    depth: depth,
                    node: this
                };
            }
    
            return argmax(this.children.map(function(child) {
                return child.getLevelDeepestLeaf(accumulatedDepth);
            }), function(result) {
                return result.depth;
            });
    
        }
    
        countBranches() : number {
            if (this.isLeafNode()) return 1;
    
            return this.children[0].countBranches() + this.children[1].countBranches();
        };
    
        toJSON() : string {
            return JSON.stringify(toPlainObject(this), null, 2);
        }
    
        static fromJSON(json : string) : GraphNode {
            return fromPlainObject(JSON.parse(json), null);
        }
    }
}

module App {
    interface Dimensions {
        width: number;
        height: number;
    }
    
    interface Point {
        x: number;
        y: number;
    }
    
    var context : CanvasRenderingContext2D;
    var canvas : HTMLCanvasElement;
    var size : Dimensions;
    var padding : number;
    var tree : Tree.GraphNode;

    export function init(config : any) {
        canvas = config.canvas;
        context = <CanvasRenderingContext2D> canvas.getContext('2d');
        size = { width: canvas.width, height: canvas.height };
        padding = 10;
    }

    export function setTreeJSON(json : string) {
        tree = Tree.GraphNode.fromJSON(json);
    }

    function drawLine(start : Point, end: Point) {
        context.beginPath();
        context.moveTo(padding + start.x, padding + start.y);
        context.lineTo(padding + end.x, padding + end.y);
        context.stroke();
    }

    function drawFilledCircle(position: Point, radius : number) {
        context.fillStyle = 'black';
        context.strokeStyle = 'black';
        context.beginPath();
        context.arc(padding + position.x, padding + position.y, radius, 0, 2 * Math.PI);
        context.fill();
    }

    function drawTextCentered(text : string, position: Point) {
        context.textAlign = 'center';
        context.fillText(text, padding + position.x, padding + position.y);
    }

    function branchTo(start : Point, end : Point, dimensions: Dimensions, distanceWidth, node) {
        drawLine(start, end);
        drawTree(end, dimensions, distanceWidth, node);
    }

    function drawTree(position : Point, dimensions : Dimensions, distanceWidth: number, node: Tree.GraphNode) {
        drawFilledCircle(position, 5);

        var nextX = position.x + node.distance * distanceWidth;
        var nextPosition = {x: nextX, y: position.y};
        drawTextCentered(node.distance.toString(), {x: position.x + (nextX - position.x) / 2, y: position.y - 5});
        drawLine(position, nextPosition);

        console.log('node with dist:', node.distance, ', height:', dimensions.height);

        if (node.children && node.children.length === 2) {
            var branchesLeft = node.children[0].countBranches(),
                branchesRight = node.children[1].countBranches();

            var branchesTotal = branchesLeft + branchesRight;

            var leftHeight = branchesLeft / branchesTotal * dimensions.height,
                rightHeight = branchesRight / branchesTotal * dimensions.height;

            console.log('\tleft child branches', branchesLeft);
            console.log('\tright child branches', branchesRight);

            branchTo(nextPosition, {x: nextX, y: position.y - (dimensions.height - leftHeight) / 2}, {width: dimensions.width, height: leftHeight}, distanceWidth, node.children[0]);
            branchTo(nextPosition, {x: nextX, y: position.y + (dimensions.height - rightHeight) / 2}, {width: dimensions.width, height: rightHeight}, distanceWidth, node.children[1]);
        }
    }

    function drawGrid(numMasks : number, horizontalAxisHeight : number) {
        // Horizontal axis
        context.strokeStyle = 'black';
        drawLine({x: 0, y: size.height}, {x: size.width - 0, y: size.height});
        
        // Draw grid
        var xStep = size.width / numMasks;
        for (var x = 0; x <= numMasks; x++) {
            context.strokeStyle = '#eee';
            drawLine({x: x * xStep, y: 0}, {x: x * xStep, y: size.height});

            context.strokeStyle = 'black';
            drawLine({x: x * xStep, y: size.height - 2.5}, {x: x * xStep, y: size.height + 2.5});
            drawTextCentered(x.toString(), {x: x * xStep, y: size.height + 12});
        }

        context.strokeStyle = '#eee';
        var yStep = size.height / numMasks;
        for (var y = 0; y <= numMasks; y++) {
            drawLine({x: 0, y: y * yStep}, {x: size.width, y: y * yStep});
        }
    }

    export function draw() {
        var horizontalAxisHeight = 20;
        size.width = canvas.width - padding * 2;
        size.height = canvas.height - horizontalAxisHeight - padding * 2;

        context.clearRect(0, 0, size.width, size.height);

        if (!tree) {
            drawTextCentered('No valid tree!', {x: size.width / 2, y: size.height / 2});
            return;
        }

        var depth = tree.getDeepestLeaf().distance;

        drawGrid(depth, horizontalAxisHeight);
        
        // Draw staring with root
        drawTree({x: 0, y: 5 + (size.height - 5) / 2}, {width: size.width, height: size.height - 10}, size.width / depth, tree);
    }
    
    interface Status {
        context: CanvasRenderingContext2D;
        size: Dimensions;
        tree: Tree.GraphNode;
    }

    export function status() : Status {
        return {
            context: context,
            size: size,
            tree: tree
        };
    }
}
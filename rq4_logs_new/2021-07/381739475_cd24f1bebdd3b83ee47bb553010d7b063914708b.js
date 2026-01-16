import makeOptionGetter from './utils'

export default class NKOGrid {
	
	constructor (params) {
	
		// Gets object fields if present, otherwise returns a default.
		const optionOr = makeOptionGetter(params);

		this.width = optionOr('width', 100);
		this.height = optionOr('height', 100);
		this.centerX = optionOr('centerX', 0);
		this.centerY = optionOr('centerY', 0);

		this.numX = optionOr('numX', 10);
		this.numY = optionOr('numY', 10);

		this.nodes = Array(numX * numY);
		populateNodes(this);

	}

	getNode (column, row) {

		return this.nodes[row * this.numX + column];

	}

	getNodeSafe (column, row) {
	
		if (column > this.numX || row > this.numY) throw new RangeError("Node selection out of bounds - getNodeSafe()");

		return getNode(column, row);

	}

}

function populateNodes (grid) {

	const spacingX = grid.width / grid.numX;
	const spacingY = grid.height / grid.numY;
	function index(col, row) { return row * grid.numX + col; }

	for (let i = 0, sX = spacingX / 2; i < grid.numX; i++, sX += spacingX) {
		for (let j = 0, sY = spacingY / 2; j < grid.numY; j++, sY += spacingY) {
			grid.nodes[index(i, j)] = {
				x: sX,
				y: sY,
				iX: i,
				iY: j,
				data: {}
			}
		}
	}
}
/* eslint-disable */

const forceBound = (
	strength = 1,
	padding = 0,
	minX = -100,
	maxX = 100,
	minY = -100,
	maxY = 100
): (() => void) => {
	let nodes = []

	const force = () => {
		nodes.forEach(node => {
			node.x = Math.max(node.x, minX + padding)
			node.x = Math.min(node.x, maxX - padding)
			node.y = Math.max(node.y, minY + padding)
			node.y = Math.min(node.y, maxY - padding)
		})
	}

	force.initialize = _nodes => {
		nodes = _nodes
	}

	force.padding = () => padding
	force.minX = () => minX
	force.maxX = () => maxX
	force.minY = () => minY
	force.strength = () => strength

	return force
}

export default forceBound
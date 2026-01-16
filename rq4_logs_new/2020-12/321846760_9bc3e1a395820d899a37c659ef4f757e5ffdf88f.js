(function (n) {
	var input = `.......#
....#...
...###.#
#...###.
....##..
##.#..#.
###.#.#.
....#...`;

// input = `.#.
// ..#
// ###`

	input = input.split("\n").map(x => x.split(""));
	
	function Cube(x, y, z) {
		var self = this;
		
		if (typeof x === "string") {
			var args = x.split(",").map(arg => +arg);
			x = args[0];
			y = args[1];
			z = args[2];
		}
		
		this.X = x;
		this.Y = y;
		this.Z = z;
		
		this.Key = function () {
			return self.X + "," + self.Y + "," + self.Z;
		}
		this.Neighbours = function () {
			var result = [];
			
			for (var i = -1; i <= 1; i++) {
				for (var j = -1; j <= 1; j++) {
					for (var k = -1; k <= 1; k++) {
						if (i !== 0 || j !== 0 || k !== 0) {
							result.push(new Cube(self.X + i, self.Y + j, self.Z + k).Key());
						}
					}
				}
			}
			
			return result;
		}
	}
	
	var cubes = input.map((x, i) => x.map((y, j) => [i,j,y])).reduce((a, b) => a.concat(b)).filter(x => x[2] === "#").map(x => new Cube(x[0], x[1], 0));
	
	function step() {
		var active = {};
		var neighbours = {};
		
		for (var i = 0; i < cubes.length; i++) {
			var cube = cubes[i];
			active[cube.Key()] = true;
			var cube_neighbours = cube.Neighbours();
			for (var j = 0; j < cube_neighbours.length; j++) {
				var key = cube_neighbours[j];
				neighbours[key] = (neighbours[key] || 0) + 1;
			}
		}
		
		var result_cubes = Object.keys(neighbours).filter(x => active[x] ? neighbours[x] === 2 || neighbours[x] === 3 : neighbours[x] === 3).map(x => new Cube(x));
		
		cubes = result_cubes;
	}
	
	for (var i = 0; i < n; i++) {
		step();
	}
	
	return cubes.length;
})(6);
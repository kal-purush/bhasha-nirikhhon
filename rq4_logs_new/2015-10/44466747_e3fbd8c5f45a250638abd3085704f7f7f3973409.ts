/// <reference path="harmony/harmony.ts" />
/// <reference path="puralax/puralax.ts" />
/// <reference path="Solver.ts" />

/*var game = new Harmony.Game();
var state = new Harmony.State(3);
state.colors = {
	'0x0': {line: 0, dots: 1},
	'1x0': { line: 0, dots: 1 },
	'2x0': { line: 0, dots: 2 },

	'0x1': { line: 1, dots: 1 },
	'1x1': { line: 1, dots: 1 },
	'2x1': { line: 1, dots: 2 },

	'0x2': { line: 2, dots: 1 },
	'1x2': { line: 2, dots: 1 },
	'2x2': { line: 2, dots: 2 },
};


var startState = state.clone();
var solver = new BackTrackingSolver.Solver(game);
var moves = solver.solve(startState);
solver.applySolution(startState, moves);*/

var game = new Puralax.Game(Puralax.Colors.Purple);
var state = new Puralax.State(5, 6);

/*state.setCells([
	{ coord: '0x0', cell: { color: Puralax.Colors.Yellow, dots: 0 } },
	{ coord: '1x0', cell: { color: Puralax.Colors.Purple, dots: 0 } },
	{ coord: '2x0', cell: { color: Puralax.Colors.Purple, dots: 0 } },
	{ coord: '3x0', cell: { color: Puralax.Colors.Blue, dots: 1 } },

	{ coord: '0x1', cell: { color: Puralax.Colors.Purple, dots: 0 } },
	{ coord: '1x1', cell: { color: Puralax.Colors.Purple, dots: 0 } },
	{ coord: '2x1', cell: { color: Puralax.Colors.Green, dots: 1 } },
	{ coord: '3x1', cell: { color: Puralax.Colors.Green, dots: 0 } },

	{ coord: '0x2', cell: { color: Puralax.Colors.Red, dots: 0 } },
	{ coord: '1x2', cell: { color: Puralax.Colors.Red, dots: 1 } },
	{ coord: '2x2', cell: { color: Puralax.Colors.Yellow, dots: 1 } },
	{ coord: '3x2', cell: { color: Puralax.Colors.Yellow, dots: 0 } },

	{ coord: '0x3', cell: { color: Puralax.Colors.Green, dots: 0 } },
	{ coord: '1x3', cell: { color: Puralax.Colors.Red, dots: 0 } },
	{ coord: '2x3', cell: { color: Puralax.Colors.Red, dots: 0 } },
	{ coord: '3x3', cell: { color: Puralax.Colors.Blue, dots: 0 } },
]);*/

state.setCells([
	{ coord: '0x0', cell: { color: Puralax.Colors.Green, dots: 1 } },
	{ coord: '1x0', cell: { color: Puralax.Colors.Red, dots: 0 } },
	{ coord: '2x0', cell: { color: Puralax.Colors.Purple, dots: 1 } },
	{ coord: '3x0', cell: { color: Puralax.Colors.Red, dots: 1 } },
	{ coord: '4x0', cell: { color: Puralax.Colors.Green, dots: 0 } },

	{ coord: '0x1', cell: { color: Puralax.Colors.Green, dots: 0 } },
	{ coord: '1x1', cell: { color: Puralax.Colors.Green, dots: 0 } },
	{ coord: '2x1', cell: { color: Puralax.Colors.Blue, dots: 0 } },
	{ coord: '3x1', cell: { color: Puralax.Colors.Green, dots: 0 } },
	{ coord: '4x1', cell: { color: Puralax.Colors.Green, dots: 0 } },

	{ coord: '0x2', cell: { color: Puralax.Colors.Red, dots: 0 } },
	{ coord: '1x2', cell: { color: Puralax.Colors.Yellow, dots: 0 } },
	{ coord: '2x2', cell: { color: Puralax.Colors.Green, dots: 0 } },
	{ coord: '3x2', cell: { color: Puralax.Colors.Yellow, dots: 1 } },
	{ coord: '4x2', cell: { color: Puralax.Colors.Red, dots: 0 } },

	{ coord: '0x3', cell: { color: Puralax.Colors.Red, dots: 0 } },
	{ coord: '1x3', cell: { color: Puralax.Colors.Yellow, dots: 0 } },
	{ coord: '2x3', cell: { color: Puralax.Colors.Green, dots: 0 } },
	{ coord: '3x3', cell: { color: Puralax.Colors.Yellow, dots: 0 } },
	{ coord: '4x3', cell: { color: Puralax.Colors.Red, dots: 0 } },

	{ coord: '0x4', cell: { color: Puralax.Colors.Green, dots: 0 } },
	{ coord: '1x4', cell: { color: Puralax.Colors.Green, dots: 0 } },
	{ coord: '2x4', cell: { color: Puralax.Colors.Blue, dots: 2 } },
	{ coord: '3x4', cell: { color: Puralax.Colors.Green, dots: 0 } },
	{ coord: '4x4', cell: { color: Puralax.Colors.Green, dots: 0 } },

	{ coord: '0x5', cell: { color: Puralax.Colors.Green, dots: 1 } },
	{ coord: '1x5', cell: { color: Puralax.Colors.Red, dots: 0 } },
	{ coord: '2x5', cell: { color: Puralax.Colors.Purple, dots: 0 } },
	{ coord: '3x5', cell: { color: Puralax.Colors.Red, dots: 1 } },
	{ coord: '4x5', cell: { color: Puralax.Colors.Green, dots: 0 } }
]);

console.log(state.toString());
var startState = state.clone();
var solver = new BackTrackingSolver.Solver(game);
var moves = solver.solve(startState);
solver.applySolution(startState, moves);
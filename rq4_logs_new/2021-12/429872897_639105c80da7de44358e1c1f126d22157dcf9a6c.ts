import chalk from "chalk";
import { performance } from "perf_hooks";
import { log, logSolution } from "../../../util/log";
import * as test from "../../../util/test";
import * as util from "../../../util/util";

const YEAR = 2021;
const DAY = 4;

type Square = number | undefined
type Board = Square[]

const winners = [
	[ 0,  1,  2,  3,  4],
	[ 5,  6,  7,  8,  9],
	[10, 11, 12, 13, 14],
	[15, 16, 17, 18, 19],
	[20, 21, 22, 23, 24],

	[ 0,  5, 10, 15, 20],
	[ 1,  6, 11, 16, 21],
	[ 2,  7, 12, 17, 22],
	[ 3,  8, 13, 18, 23],
	[ 4,  9, 14, 19, 24],
]

// solution path: /Users/ccottingham/Projects/advent-of-code/2021/years/2021/04/index.ts
// data path    : /Users/ccottingham/Projects/advent-of-code/2021/years/2021/04/data.txt
// problem url  : https://adventofcode.com/2021/day/4

async function p2021day4_part1(input: string, ...params: any[]) {
	const matchData = input.trim().match(/^(\S+)\s+(.+)$/sm)
	if (!matchData) {
		return "no match in input"
	}

	const drawStr = matchData[1]
	const boardStrs = matchData[2].split(/\n\n+/)

	const draws = drawStr.split(",").map(Number)
	const boards: Board[] = boardStrs.map((s) => s.trim().split(/\s+/).map(Number))

	let solution = undefined
	for (let i = 0; i < draws.length && !solution; ++i) {
		const draw = draws[i]
		for (let j = 0; j < boards.length && !solution; ++j) {
			const board = boards[j]
			const index = board.indexOf(draw)
			if (index >= 0) {
				board[index] = undefined
				if (isWinner(board)) {
					const remaining = board.reduce((acc: number, n: Square) => acc + (n || 0), 0)
					solution = draw * remaining
					break
				}
			}
		}
	}

	return solution
}

async function p2021day4_part2(input: string, ...params: any[]) {
	return "Not implemented";
}

function isWinner(board: Board): boolean {
	return winners.some((indices) => {
		return indices.every((n) => board[n] === undefined)
	})
}

async function run() {
	const part1tests: TestCase[] = [
		{
			input: `
7,4,9,5,11,17,23,2,0,14,21,24,10,16,13,6,15,25,12,22,18,20,8,19,3,26,1

22 13 17 11  0
 8  2 23  4 24
21  9 14 16  7
 6 10  3 18  5
 1 12 20 15 19

 3 15  0  2 22
 9 18 13 17  5
19  8  7 25 23
20 11 10 24  4
14 21 16 12  6

14 21 17 24  4
10 16 15  9 19
18  8 23 26 20
22 11 13  6  5
 2  0 12  3  7
			`,
			expected: "4512"
		}
	];
	const part2tests: TestCase[] = [];

	// Run tests
	test.beginTests();
	await test.section(async () => {
		for (const testCase of part1tests) {
			test.logTestResult(testCase, String(await p2021day4_part1(testCase.input, ...(testCase.extraArgs || []))));
		}
	});
	await test.section(async () => {
		for (const testCase of part2tests) {
			test.logTestResult(testCase, String(await p2021day4_part2(testCase.input, ...(testCase.extraArgs || []))));
		}
	});
	test.endTests();

	// Get input and run program while measuring performance
	const input = await util.getInput(DAY, YEAR);

	const part1Before = performance.now();
	const part1Solution = String(await p2021day4_part1(input));
	const part1After = performance.now();

	const part2Before = performance.now()
	const part2Solution = String(await p2021day4_part2(input));
	const part2After = performance.now();

	logSolution(4, 2021, part1Solution, part2Solution);

	log(chalk.gray("--- Performance ---"));
	log(chalk.gray(`Part 1: ${util.formatTime(part1After - part1Before)}`));
	log(chalk.gray(`Part 2: ${util.formatTime(part2After - part2Before)}`));
	log();
}

run()
	.then(() => {
		process.exit();
	})
	.catch(error => {
		throw error;
	});
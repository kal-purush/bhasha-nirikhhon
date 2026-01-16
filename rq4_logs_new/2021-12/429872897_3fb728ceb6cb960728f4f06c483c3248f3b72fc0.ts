import chalk from "chalk"
import { performance } from "perf_hooks"
import { log, logSolution } from "../../../util/log"
import * as test from "../../../util/test"
import * as util from "../../../util/util"

const YEAR = 2021
const DAY = 7

// solution path: /Users/ccottingham/Projects/advent-of-code/2021/years/2021/07/index.ts
// data path    : /Users/ccottingham/Projects/advent-of-code/2021/years/2021/07/data.txt
// problem url  : https://adventofcode.com/2021/day/7

async function p2021day7_part1(input: string, ...params: any[]): Promise<string> {
	const data = util.numberify(input)
	const position = params[0]

	if (position !== undefined) {
		return totalFuelToPosition(data, position).toString()
	}

	const min = util.min(data).value
	const max = util.max(data).value
	let minFuel = max * data.length

	for (let p = min; p <= max; ++p) {
		const fuel = totalFuelToPosition(data, p)
		if (fuel < minFuel) { minFuel = fuel }
		if (fuel > minFuel) { break }
	}

	return `${minFuel}`
}

async function p2021day7_part2(input: string, ...params: any[]): Promise<string> {
	return "Not implemented"
}

function totalFuelToPosition(data: number[], position: number): number {
	return data.map((p: number) => Math.abs(p - position)).reduce((acc: number, p: number) => acc + p)
}

async function run() {
	const testData = "16,1,2,0,4,2,7,1,2,14"

	const part1tests: TestCase[] = [
		{
			input: testData,
			expected: "41",
			extraArgs: [ 1 ]		// position on which to align
		},
		{
			input: testData,
			expected: "37",
			extraArgs: [ 2 ]		// position on which to align
		},
		{
			input: testData,
			expected: "39",
			extraArgs: [ 3 ]		// position on which to align
		},
		{
			input: testData,
			expected: "71",
			extraArgs: [ 10 ]		// position on which to align
		},
		{
			input: testData,
			expected: "37",
			// no extra arg means find the minimum
		}
	]
	const part2tests: TestCase[] = []

	// Run tests
	test.beginTests()
	await test.section(async () => {
		for (const testCase of part1tests) {
			test.logTestResult(testCase, String(await p2021day7_part1(testCase.input, ...(testCase.extraArgs || []))))
		}
	})
	await test.section(async () => {
		for (const testCase of part2tests) {
			test.logTestResult(testCase, String(await p2021day7_part2(testCase.input, ...(testCase.extraArgs || []))))
		}
	})
	test.endTests()

	// Get input and run program while measuring performance
	const input = await util.getInput(DAY, YEAR)

	const part1Before = performance.now()
	const part1Solution = String(await p2021day7_part1(input))
	const part1After = performance.now()

	const part2Before = performance.now()
	const part2Solution = String(await p2021day7_part2(input))
	const part2After = performance.now()

	logSolution(7, 2021, part1Solution, part2Solution)

	log(chalk.gray("--- Performance ---"))
	log(chalk.gray(`Part 1: ${util.formatTime(part1After - part1Before)}`))
	log(chalk.gray(`Part 2: ${util.formatTime(part2After - part2Before)}`))
	log()
}

run()
	.then(() => {
		process.exit()
	})
	.catch(error => {
		throw error
	})
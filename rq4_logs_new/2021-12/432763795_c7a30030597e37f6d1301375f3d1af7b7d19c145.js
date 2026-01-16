import { loadInput } from '../common/input.js';
import { parseInput } from './utils.js';

const input = loadInput(1);
const [polymer, insertionRules] = parseInput(input);

const elementCounter = new Map();
let pairCounter = new Map();

const elements = polymer.split('');

for (let i = 0; i < elements.length; i++) {
	const element = elements[i];
	const next = elements[i + 1];

	const elementCount = elementCounter.get(element) ?? 0;
	elementCounter.set(element, elementCount + 1);

	if (!next) {
		continue;
	}

	const pair = element + next;
	const pairCount = pairCounter.get(pair) ?? 0;

	pairCounter.set(pair, pairCount + 1);
}

const steps = 40;

for (let i = 0; i < steps; i++) {
	const newPairCounter = new Map();

	for (const [pair, count] of pairCounter) {
		const elementToInsert = insertionRules.get(pair);

		if (!elementToInsert) {
			const newCount = newPairCounter.get(pair) ?? 0;
			newPairCounter.set(pair, newCount, +count);
			continue;
		}

		const elementCount = elementCounter.get(elementToInsert) ?? 0;
		elementCounter.set(elementToInsert, elementCount + count);

		const newPairs = [pair[0] + elementToInsert, elementToInsert + pair[1]];

		for (const newPair of newPairs) {
			const newCount = newPairCounter.get(newPair) ?? 0;
			newPairCounter.set(newPair, newCount + count);
		}
	}

	pairCounter = newPairCounter;
}

const sortedElementCounts = Array.from(elementCounter.values()).sort(
	(a, b) => a - b,
);

const min = sortedElementCounts.shift();
const max = sortedElementCounts.pop();

const score = max - min;

console.log(score);
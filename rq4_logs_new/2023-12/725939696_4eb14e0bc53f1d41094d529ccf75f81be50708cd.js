export function partOne(input) {
	const lines = input.split("\n");

	const [instructions, _, ...map] = lines;

	const letterMap = Object.fromEntries(
		map.map((line) => {
			const [_, start, left, right] =
				/([\w]{3}) = \(([\w]{3}), ([\w]{3})\)/.exec(line);

			return [start, { left, right }];
		})
	);

	let steps = 0;
	let current = "AAA";
	let index = 0;

	while (true) {
		if (current === "ZZZ") break;

		const next = instructions[index];

		const here = letterMap[current];

		if (next === "L") {
			current = here.left;
		} else {
			current = here.right;
		}

		steps++;
		index++;

		if (index >= instructions.length) index = 0;
	}

	return steps;
}

export function partTwoOld(input) {
	const lines = input.split("\n");

	const [instructions, _, ...map] = lines;

	const letterMap = Object.fromEntries(
		map.map((line) => {
			const [_, start, left, right] =
				/([\w]{3}) = \(([\w]{3}), ([\w]{3})\)/.exec(line);

			return [start, { left, right }];
		})
	);

	let steps = 0;
	let current = "AAA";
	let currentNodes = Object.keys(letterMap).filter((letters) =>
		letters.endsWith("A")
	);
	let index = 0;
	let total = 0;

	while (true) {
		// console.log(currentNodes);
		if (currentNodes.every((node) => node.endsWith("Z"))) break;

		const next = instructions[index];

		currentNodes.forEach((node, i) => {
			const here = letterMap[node];

			if (next === "L") {
				currentNodes[i] = here.left;
			} else {
				currentNodes[i] = here.right;
			}
		});

		steps++;
		index++;
		total++;

		// if (total > 100000) break;

		if (index >= instructions.length) index = 0;
	}

	return steps;
}

export function partTwo(input) {
	const lines = input.split("\n");

	const [instructions, _, ...map] = lines;

	const letterMap = Object.fromEntries(
		map.map((line) => {
			const [_, start, left, right] =
				/([\w]{3}) = \(([\w]{3}), ([\w]{3})\)/.exec(line);

			return [start, { left, right }];
		})
	);

	let currentNodes = Object.keys(letterMap).filter((letters) =>
		letters.endsWith("A")
	);

	const cache = new Map();

	let currentSteps = currentNodes.map((x) => 0);

	while (
		!currentSteps.reduce((a, b) => (a === b ? a : null)) &&
		currentSteps[0] < 1_000_000
	) {
		let minIndex = 0;

		console.log(currentSteps, currentNodes);

		currentSteps.forEach((step, i) => {
			if (step < currentSteps[minIndex]) minIndex = i;
		});

		const next = nextZ(
			currentNodes[minIndex],
			currentSteps[minIndex] % instructions.length,
			instructions,
			letterMap,
			cache
		);

		currentNodes[minIndex] = next.key;
		currentSteps[minIndex] += next.steps;
	}

	return currentSteps[0];
}

function nextZ(letters, index, instructions, letterMap, cache) {
	if (!cache.has(letters)) cache.set(letters, new Map());
	const letterIndexMap = cache.get(letters);

	if (letterIndexMap.has(index)) return letterIndexMap.get(index);

	const here = letterMap[letters];

	const next = instructions[index] === "L" ? here.left : here.right;

	if (next.endsWith("Z")) {
		letterIndexMap.set(index, { key: next, steps: 1 });

		return letterIndexMap.get(index);
	} else {
		letterIndexMap.set(index, { key: next, steps: NaN });

		const nextNextZ = nextZ(
			next,
			index + 1 >= instructions.length ? 0 : index + 1,
			instructions,
			letterMap,
			cache
		);

		letterIndexMap.set(index, {
			key: nextNextZ.key,
			steps: nextNextZ.steps + 1,
		});

		return letterIndexMap.get(index);
	}
}

function primeFactors(number) {
	const factors = {};

	for (let i = 2; i <= number; i++) {
		while (number % i === 0) {
			if (!factors[i]) factors[i] = 0;
			factors[i]++;
			number /= i;
		}
	}

	return factors;
}

function leastCommonMultiple(array) {
	const primeFactorizations = array.map(primeFactors);

	const highestPowers = {};

	primeFactorizations.forEach((factorization) => {
		Object.keys(factorization).forEach((prime) => {
			if (
				!highestPowers[prime] ||
				factorization[prime] > highestPowers[prime]
			) {
				highestPowers[prime] = factorization[prime];
			}
		});
	});

	let product = 1;

	Object.entries(highestPowers).forEach(([prime, power]) => {
		product *= prime ** power;
	});

	return product;
}
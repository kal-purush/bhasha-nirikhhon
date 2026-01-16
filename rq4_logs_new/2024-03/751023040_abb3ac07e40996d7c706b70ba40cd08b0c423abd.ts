//import random from 'random';

class UnitError extends Error {}

const tolerance = 0.01;
//TODO: fix imports.. oh god..
export class Dice {
    readonly sides: number;
    readonly pmf: Array<number>;
    readonly cdf: Array<number>;

    //private readonly rng = random.uniform(0, 1);

    constructor(numSides: number, weights?: Array<number>) {
        this.sides = numSides;
        this.pmf = weights ? weights : new Array(numSides).fill(1/numSides)
        let unitSum = this.pmf.reduce((acc, x) => acc + x)
        if (Math.abs(1 - unitSum) >= tolerance) {
            throw new UnitError("Sum of weights does not equal 1.");
        }
        this.cdf = this.pmf.map((sum => value => sum += value)(0));
    }

    roll(): number {
        return this.cdf.findIndex(el => Math.random() <= el) + 1;
    }
    
}
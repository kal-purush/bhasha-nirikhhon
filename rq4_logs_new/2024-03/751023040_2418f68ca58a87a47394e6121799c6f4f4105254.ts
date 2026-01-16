import { Dice } from "../src/classes/Dice/Dice"
const sides = 6;
const diceName = `D${sides}`;
let dn = new Dice(sides);

describe('DiceRoller upon Initialization:', () => {
    test(`${diceName} shall have ${sides} PMF values.`, () => {
        expect(dn.pmf).toHaveLength(sides);
    });
    
    test(`${diceName} shall have ${sides} CDF values.`, () => {
        expect(dn.cdf).toHaveLength(sides);
    });

    test(`${diceName}'s PMF values shall sum to 1.`, () => {
        expect(dn.pmf.reduce((acc, x) => acc + x)).toBeCloseTo(1);
    });
});

describe('DiceRoller Behavior Testing.', () => {
    const trials = 1000;
    const expectedFreq = 1/sides;

    let diceRolls: Array<number> = new Array(trials);
    let rollCounts: Array<number> = new Array(sides).fill(0);

    for (let i = 0; i < trials; i++) {
        const result = dn.roll();
        diceRolls[i] = result;
        rollCounts[result - 1]++;
    }
    
    test(`${diceName} shall produce outputs within range [1,${sides}].`, () => {
        for (let i = 0; i < sides; i++) {
            expect(diceRolls).toContain(i + 1);
        }
    });

    test(`${diceName} shall have a uniform distribution among dice rolls.`, () => {
        for (let count of rollCounts) {
           const experimentalFreq = count/trials;
           expect(Math.abs(experimentalFreq - expectedFreq)).toBeCloseTo(0, 0); // distribution should match up within epsilon=0.5
        }
    });
});
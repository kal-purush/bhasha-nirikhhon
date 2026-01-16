import { createReadStream } from 'fs';
import { resolve } from 'path';
import { createInterface } from 'readline';

type Instruction = {
  action: 'set' | 'sub' | 'mul' | 'jnz';
  args: (string|number)[];
};

type Register = 'a' | 'b' | 'c' | 'd' | 'e' | 'f' | 'g' | 'h';

const getInstructions = (file: string): Promise<{registers: {[key in Register]: number}, instructions: Instruction[]}> =>
  new Promise((res) => {
    const registers = { a: 0, b: 0, c: 0, d: 0, e: 0, f: 0, g: 0, h: 0 };
    const instructions: Instruction[] = [];
    const rl = createInterface({
      crlfDelay: global.Infinity,
      input: createReadStream(resolve(__dirname, `../../resources/23-coprocessor-conflagration/${file}.txt`)),
    } as any);

    rl.on('line', (line: string) => {
      const [action, arg1, arg2] = line.split(' ');
      const a1 = parseInt(arg1, 10) || arg1;
      const a2 = parseInt(arg2, 10) || arg2;
      const instruction: Instruction = {
        action: action as Instruction['action'],
        args: [a1, a2],
      };

      instructions.push(instruction);
    });

    rl.on('close', () => res({ registers, instructions }));
  });

const getFirstRecoveredFrequency = async (file: string): Promise<number> => {
  const { registers, instructions } = await getInstructions(file);
  const getValue = (a: string|number): number => (Number.isInteger(a as number) ? a : registers[a]) as number;
  let totalMul = 0;

  for (let i = 0; i >= 0 && i < instructions.length; ++i) {
    const instruction = instructions[i];
    const stringInstruction = i + '-';

    switch (instruction.action) {
      case 'jnz': {
        const regValue = getValue(instruction.args[0]);

        if (regValue !== 0) {
          i += (getValue(instruction.args[1]) - 1);
        }

        break;
      }
      case 'mul': {
        registers[instruction.args[0]] *= getValue(instruction.args[1]);
        ++totalMul;
        break;
      }
      case 'set': {
        registers[instruction.args[0]] = getValue(instruction.args[1]);
        break;
      }
      case 'sub': {
        registers[instruction.args[0]] -= getValue(instruction.args[1]);
        break;
      }
      default:
        throw new Error('Invalid instruction');
    }
  }

  return totalMul;
};

/* PART TWO */

/**
 * Returns the total of nonPrimes among the numbers between min and max with specified increment
 * See comment at the end of current script to see non-optimized translated code from source program.txt
 *
 * @param {number} [min=105700] starting number to be tested (formerly b)
 * @param {number} [max=122700] max number to stop at (formerly c)
 * @param {number} [increment=17] increment used to reach max starting from min
 * @returns {number}
 */
const getTotalNonPrimes = (min = 105700, max = 122700, increment = 17): number => {
  let divider = 0; // formerly d
  let nonPrimes = 0; // formerly h

  for (let i = min; i <= max; i += increment) { // <= as increment occurs after checking equality in source code
    for (divider = 2; divider < i; ++divider) {
      if (i % divider === 0) {
        ++nonPrimes;
        break;
      }
    }
  }

  return nonPrimes;
};

(async function () {
  console.log(`Result: ${await getFirstRecoveredFrequency('program')}`);
  console.log(`Result: ${getTotalNonPrimes()}`);
})();

/*
  // === program.txt translated into non optimized code: ===

  function getTotalNonPrimesSlow(a = 1) {
    let b = 57;
    let c = 57;
    let d = 0;
    let e = 0;
    let f = 0;
    let g = 0;
    let h = 0;

    if (a === 1) {
      b = b * 100 + 100000;
      c = b + 17000;
    }

    do {
      f = 1;
      d = 2;

      do {
        e = 2;

        do {
          g = d * e - b;

          if (g === 0) {
            f = 0;
          }

          ++e;
          g = e - b;
        } while (g !== 0); // loop 3: test all factors of d with variable e

        ++d;
        g = d - b;
      } while (g !== 0); // loop 2: checks if b is a factor of d (modulo) using loop 3, incrementing variable e

      if (f === 0) {
        ++h;
      }

      g = b - c;
      b += 17;
    } while (g !== 0); // loop 1: iterates from b to c by jumps of 17

    return h;
  }
*/
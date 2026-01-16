type State = 'A' | 'B' | 'C' | 'D' | 'E' | 'F';

const getTuringMachineChecksum = (checksumStepCheck = 12964419): number => {
  const tape: number[] = Array(checksumStepCheck).fill(0);
  let state = 'A' as State;
  let steps = 0;
  let i = Math.floor(tape.length / 2);

  while (++steps <= checksumStepCheck) {
    switch (state) {
      case 'A':
        if (tape[i] === 0) {
          tape[i++] = 1;
          state = 'B';
        } else {
          tape[i++] = 0;
          state = 'F';
        }
        break;
      case 'B':
        if (tape[i] === 0) {
          tape[i--] = 0;
          state = 'B';
        } else {
          tape[i--] = 1;
          state = 'C';
        }
        break;
      case 'C':
        if (tape[i] === 0) {
          tape[i--] = 1;
          state = 'D';
        } else {
          tape[i++] = 0;
          state = 'C';
        }
        break;
      case 'D':
        if (tape[i] === 0) {
          tape[i--] = 1;
          state = 'E';
        } else {
          tape[i++] = 1;
          state = 'A';
        }
        break;
      case 'E':
        if (tape[i] === 0) {
          tape[i--] = 1;
          state = 'F';
        } else {
          tape[i--] = 0;
          state = 'D';
        }
        break;
      case 'F':
        if (tape[i] === 0) {
          tape[i++] = 1;
          state = 'A';
        } else {
          tape[i--] = 0;
          state = 'E';
        }
        break;
      default:
        throw new Error('Invalid state.');
    }
  }

  return tape.reduce((acc, val) => val === 1 ? acc + 1 : acc, 0);
};

console.log(getTuringMachineChecksum());
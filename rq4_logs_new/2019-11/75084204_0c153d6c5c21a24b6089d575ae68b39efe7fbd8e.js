import { splitLines } from 'utils/strings';
import input from './input';
import _ from 'lodash';

// -------------------------------------
// PART 1
// -------------------------------------

const parsedInput = splitLines(input, (string) => {
  const reg = new RegExp(
    /^Step ([A-Z]{1}) must be finished before step ([A-Z]{1}) can begin.$/,
    'g',
  );
  const [fullString, parent, child] = reg.exec(string);
  return { parent, child };
});

const stepsTree = parsedInput.reduce((acc, cur) => {
  // Create Parent Node
  if (!Object.keys(acc).includes(cur.parent)) {
    acc[cur.parent] = { completed: false, assigned: false, parents: [] };
  }

  // Create or Modify a child Node
  if (!Object.keys(acc).includes(cur.child)) {
    acc[cur.child] = { completed: false, assigned: false, parents: [cur.parent] };
  } else {
    acc[cur.child].parents.push(cur.parent);
  }

  return acc;
}, {});

const areCompleted = (ids, self) => !ids.some(id => self[id].completed === false);

const nextItems = items =>
  Object.entries(items)
    .reduce((acc, [key, val]) => {
      if (!val.completed && !val.assigned && areCompleted(val.parents, items)) {
        acc.push(key);
      }
      return acc;
    }, [])
    .sort();

const completeNextStep = (items) => {
  const nextKey = nextItems(items)[0];
  if (nextKey) {
    items[nextKey].completed = true;
    return nextKey;
  }
  return false;
};

const completeSteps = (steps) => {
  let solution = '';
  let s = completeNextStep(steps);
  while (s) {
    solution = `${solution}${s}`;
    s = completeNextStep(steps);
  }
  return solution;
};

const solution1 = completeSteps(stepsTree);
console.log('Part 1:', solution1);

// Reset
Object.keys(stepsTree).forEach((s) => {
  stepsTree[s].completed = false;
});

console.log(stepsTree);
// -------------------------------------
// PART 2
// -------------------------------------

class Queue {
  constructor(options) {
    this.stepsTree = options.stepsTree;
    this.workers = new Array(options.workers).fill(null);
    this.time = 0;
    this.history = [];
  }

  tick() {
    this.time += 1;
    this.executeJobs();

    if (this.workers.filter(w => w !== null).length) {
      this.tick();
    }
  }

  executeJobs() {
    console.log(this.time, this.workers.filter(w => w !== null).length);
    this.workers.forEach((w, i, workers) => {
      if (w && this.time - w.start >= w.length) {
        this.stepsTree[w.step].completed = true;
        this.history.push(w.step);
        workers[i] = null;
        this.assignJobs();
      }
    });
  }

  assignJobs() {
    const next = nextItems(this.stepsTree);
    console.log('next length', next.length);
    // console.log(next);
    if (next.length === 0) {
      return;
    }

    this.workers = this.workers.map((w) => {
      if (w === null) {
        const step = next.splice(0, 1)[0];
        if (step) {
          this.stepsTree[step].assigned = true;
          return { step, length: step.codePointAt() - 4, start: this.time + 1 };
        }
      }
      return w;
    });
  }

  start() {
    this.assignJobs();
    this.tick();

    return { time: this.time, history: this.history.join('') };
  }
}

const myQueue = new Queue({ workers: 5, stepsTree });
const solution2 = myQueue.start();
console.log('Part 2:', solution2);
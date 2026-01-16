import { createReadStream } from 'fs';
import { resolve } from 'path';
import { createInterface } from 'readline';

type Bridge = {
  id: number,
  data: [number, number],
};

type Store = {
  [key: number]: number[],
};

const getInstructions = (file: string): Promise<{components: Bridge[], starters: Bridge[], store: Store}> => new Promise((res) => {
  const components: Bridge[] = [];
  const starters: Bridge[] = [];
  const store: Store = {};
  const rl = createInterface({
    crlfDelay: global.Infinity,
    input: createReadStream(resolve(__dirname, `../../resources/24-electromagnetic-moat/${file}.txt`)),
  } as any);

  rl.on('line', (line: string) => {
    const [b1, b2] = line.split('/').map(d => parseInt(d, 10));
    const id = components.length;
    const bridge: Bridge = { id, data: [b1, b2] };

    if (b1 === 0 || b2 === 0) {
      starters.push(bridge);
    }

    components.push(bridge);
    store[b1] = store[b1] ? [...store[b1], id] : [id];

    if (b1 !== b2) {
      store[b2] = store[b2] ? [...store[b2], id] : [id];
    }
  });

  rl.on('close', () => res({ components, starters, store }));
});

const printBridge = (bridge: Bridge[]): void => {
  let bridgeString = '';
  bridge.forEach((comp, i) => {
    bridgeString += comp.data[0] + '/' + comp.data[1] + (i === bridge.length - 1 ? '' : '--');
  });
  console.log(bridgeString);
};

const removeFromStore = (store: Store, component: Bridge): void => {
  let indexToRemove = store[component.data[0]].findIndex(c => c === component.id);

  if (indexToRemove >= 0) {
    store[component.data[0]].splice(indexToRemove, 1);
  }

  indexToRemove = store[component.data[1]].findIndex(c => c === component.id);

  if (indexToRemove >= 0) {
    store[component.data[1]].splice(indexToRemove, 1);
  }
};

const getStrongestBridge = async (file: string, isPartTwo = false): Promise<number> => {
  const { components, starters, store } = await getInstructions(file);
  let bridgeStrength = 0;
  let bridgeLength = 0;

  const updateBridgeStrength = (bridge: Bridge[]): void => {
    const strength = bridge.reduce((acc, b) => b.data[0] + b.data[1] + acc, 0);

    if (isPartTwo) {
      if (bridge.length > bridgeLength) {
        bridgeLength = bridge.length;
        bridgeStrength = strength;
        // printBridge(bridge);
      } else if (bridge.length === bridgeLength && bridgeStrength < strength) {
        bridgeStrength = strength;
        // printBridge(bridge);
      }
    } else if (bridgeStrength < strength) {
      bridgeStrength = strength;
      // printBridge(bridge);
    }
  };

  const bridgeTraversal = (bridge: Bridge[], nextNum: number, store: Store): void => {
    const levelQueue = [...store[nextNum]];

    while (levelQueue.length > 0) {
      const nextComponentIndex = levelQueue.pop();

      if (nextComponentIndex !== undefined) {
        const nextComponentData = components[nextComponentIndex].data;
        const nextNextNum = nextComponentData[0] === nextNum ? nextComponentData[1] : nextComponentData[0];
        const nextBridge = bridge.concat(components[nextComponentIndex]);

        removeFromStore(store, components[nextComponentIndex]);
        bridgeTraversal(nextBridge, nextNextNum, store);
      }
    }

    updateBridgeStrength(bridge);

    // backtrack: restore component in store
    const lastComponent = bridge[bridge.length - 1];
    const storeIndex = lastComponent.data[0] === nextNum ? lastComponent.data[1] : lastComponent.data[0];
    store[storeIndex].push(lastComponent.id);
    store[nextNum].push(lastComponent.id);
  };

  starters.forEach((starter) => {
    const innerStore = Object.keys(store).reduce(
      (acc, k) => ({ ...acc, [k]: [...store[k]] }),
      {});

    removeFromStore(innerStore, starter);
    bridgeTraversal([starter], starter.data[0] || starter.data[1], innerStore);
  });

  return bridgeStrength;
};

(async function () {
  console.log(`Test should be 31: ${await getStrongestBridge('test')}`);
  console.log(`Result: ${await getStrongestBridge('components')}`);
  console.log(`Test should be 19: ${await getStrongestBridge('test', true)}`);
  console.log(`Result: ${await getStrongestBridge('components', true)}`);
})();
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { reactive, subscription } from '../__tests__/utils/instrumented-hooks.js';
import { setConfig, runBatch } from '../config.js';
import { PersistenceStore } from '../config.js';
import { getStorageKey, serialize, type PersistedValue } from '../internals/persistence.js';
import { hashArray } from '../internals/utils/hash.js';
import { state } from '../hooks.js';
import { nextTick } from './utils/async.js';

function storageKey(key: string, args: unknown[]) {
  return getStorageKey(key, hashArray(args));
}

// Enhanced mock persistence store for testing hydration
class MockStore implements PersistenceStore {
  private store = new Map<string, PersistedValue<unknown>>();

  // Spy functions for tracking hydration
  get = vi.fn((key: string): PersistedValue<unknown> | undefined => {
    return this.store.get(key) as PersistedValue<unknown> | undefined;
  });

  set = vi.fn((key: string, value: PersistedValue<unknown>): void => {
    this.store.set(key, value);
  });

  // Counters for hydration tracking
  hydrateCallCount = 0;
  dehydrateCallCount = 0;

  // Helper to reset counters
  resetCounters() {
    this.hydrateCallCount = 0;
    this.dehydrateCallCount = 0;
    this.get.mockClear();
    this.set.mockClear();
  }

  clear = vi.fn((): void => {
    this.store.clear();
    this.resetCounters();
  });

  // Helper for tests to simulate store failure
  enableFailMode() {
    this.get.mockImplementation(() => {
      throw new Error('Storage error');
    });
    this.set.mockImplementation(() => {
      throw new Error('Storage error');
    });
  }

  disableFailMode() {
    this.get.mockImplementation((key: string) => this.store.get(key) || undefined);
    this.set.mockImplementation((key: string, value: PersistedValue<unknown>) => this.store.set(key, value));
  }

  // Helper to get all stored data for assertions
  getAll(): Record<string, unknown> {
    const result: Record<string, unknown> = {};
    this.store.forEach((value, key) => {
      try {
        result[key] = value;
      } catch (e) {
        result[key] = value;
      }
    });
    return result;
  }

  // Helper to manually add an entry
  addEntry(key: string, value: unknown, isPromise = false) {
    this.store.set(key, [isPromise ? 'promise' : 'value', value]);
  }
}

describe('Persistence Hydration', () => {
  let store: MockStore;

  beforeEach(() => {
    store = new MockStore();
    setConfig({
      persistenceStore: store,
    });
  });

  afterEach(() => {
    store.clear();
    setConfig({
      persistenceStore: undefined,
    });
    vi.restoreAllMocks();
  });

  describe('Hydration tracking semantics for derived signals with state dependencies', () => {
    it('hydrate function is tracked, and when state changes, reactive function is called again but hydrate is not called again', () => {
      // Create a state signal that will be used inside the reactive function
      const stateCount = state(0);

      // Spy functions for hydration
      const hydrateSpy = vi.fn(value => {
        stateCount.get();
        return value;
      });
      const dehydrateSpy = vi.fn(value => value);

      // Pre-populate the store
      store.addEntry(storageKey('counter-plus', [1]), 1);

      // Create a reactive function with persistence that depends on state
      const derivedPersisted = reactive((n: number) => stateCount.get() + n, {
        persist: {
          key: 'counter-plus',
          hydrate: hydrateSpy,
          dehydrate: dehydrateSpy,
        },
      });

      // First access - should use hydrated value
      const initialValue = derivedPersisted.withParams(1)();
      expect(initialValue).toBe(1);
      expect(hydrateSpy).toHaveBeenCalledTimes(1);
      expect(derivedPersisted).toHaveCounts({ compute: 0 }); // Should not compute initially

      // Reset counters for clarity
      hydrateSpy.mockClear();

      // Update the state - should trigger reactive update but not hydration
      stateCount.set(5);
      runBatch(() => {});

      // Access again - should recompute but not re-hydrate
      const updatedValue = derivedPersisted.withParams(1)();
      expect(updatedValue).toBe(6); // 5 + 1
      expect(hydrateSpy).not.toHaveBeenCalled(); // Hydrate should not be called again
      expect(derivedPersisted.withParams(1)).toHaveCounts({ compute: 1 }); // Should compute once after state change

      // Verify dehydrate was called with the new value
      expect(dehydrateSpy).toHaveBeenCalledWith(6, 1);
    });

    it('hydration establishes reactive dependencies correctly', () => {
      // Create multiple state signals
      const stateA = state(1);
      const stateB = state(10);

      // Create a hydrate function that uses both states
      const hydrateSpy = vi.fn(value => {
        // Establish dependencies on both states
        stateA.get();
        stateB.get();
        return value;
      });

      // Pre-populate the store
      store.addEntry(storageKey('multi-state', []), 10);

      // Create a reactive function with persistence
      const derivedMultiState = reactive(() => stateA.get() * stateB.get(), {
        persist: {
          key: 'multi-state',
          hydrate: hydrateSpy,
        },
      });

      // First access - should use hydrated value and establish dependencies
      const initialValue = derivedMultiState();
      expect(initialValue).toBe(10);
      expect(hydrateSpy).toHaveBeenCalledTimes(1);

      // Update stateA - should trigger recompute
      stateA.set(2);
      runBatch(() => {});
      expect(derivedMultiState()).toBe(20); // 2 * 10

      // Update stateB - should trigger recompute
      stateB.set(5);
      runBatch(() => {});
      expect(derivedMultiState()).toBe(10); // 2 * 5

      // Verify hydrate was only called once at the beginning
      expect(hydrateSpy).toHaveBeenCalledTimes(1);
    });
  });

  describe('Hydration tracking semantics for reactive functions', () => {
    it('hydrate function is called once, reactive function recomputes without re-hydrating', () => {
      const stateCount = state(0);

      // Spy functions for hydration
      const hydrateSpy = vi.fn(value => {
        stateCount.get();
        return value * 2;
      }); // Double the stored value
      const dehydrateSpy = vi.fn(value => {
        stateCount.get();
        return value / 2;
      }); // Store half the value

      // Pre-populate the store with a value
      store.addEntry(storageKey('simple-value', []), 21);

      // Create a simple reactive function with persistence
      const simpleReactive = reactive(
        () => {
          return 42;
        },
        {
          persist: {
            key: 'simple-value',
            hydrate: hydrateSpy,
            dehydrate: dehydrateSpy,
          },
        },
      );

      // First access - should use hydrated value
      const initialValue = simpleReactive();
      expect(initialValue).toBe(42); // 21 * 2 from hydrate
      expect(hydrateSpy).toHaveBeenCalledTimes(1);
      expect(simpleReactive).toHaveCounts({ compute: 0 }); // Should not compute initially

      // Access again - should not recompute or re-hydrate
      const secondValue = simpleReactive();
      expect(secondValue).toBe(42);
      expect(hydrateSpy).toHaveBeenCalledTimes(1); // Still just once
      expect(simpleReactive).toHaveCounts({ compute: 0 }); // Still not computed

      // Update the state - should trigger reactive update but not hydration
      stateCount.set(1);

      // Access again - should compute now
      const thirdValue = simpleReactive();
      expect(thirdValue).toBe(42);
      expect(simpleReactive).toHaveCounts({ compute: 1 }); // Now computed
      expect(dehydrateSpy).toHaveBeenCalledWith(42); // Should dehydrate the computed value
    });

    it('reactive function with parameters hydrates correctly for different param sets', () => {
      // Create a hydrate spy that returns different values based on parameters
      const hydrateSpy = vi.fn((value, factor) => value * factor);

      // Pre-populate the store with different values for different params
      store.addEntry(storageKey('multiply', [2]), 4); // For factor=2
      store.addEntry(storageKey('multiply', [3]), 9); // For factor=3

      // Create a reactive function with parameters
      const multiply = reactive(
        (factor: number) => {
          return 10 * factor; // This should not be called if hydrated
        },
        {
          persist: {
            key: 'multiply',
            hydrate: hydrateSpy,
          },
        },
      );

      // Access with different parameters
      const value1 = multiply.withParams(2)();
      const value2 = multiply.withParams(3)();

      // Verify hydration worked correctly
      expect(value1).toBe(8); // 4 * 2 from hydrate
      expect(value2).toBe(27); // 9 * 3 from hydrate
      expect(hydrateSpy).toHaveBeenCalledTimes(2);
      expect(multiply.withParams(2)).toHaveCounts({ compute: 0 });
      expect(multiply.withParams(3)).toHaveCounts({ compute: 0 });

      // Access with a parameter that wasn't pre-populated
      const value3 = multiply.withParams(4)();

      // Should compute the value
      expect(value3).toBe(40); // 10 * 4 from compute
      expect(multiply.withParams(4)).toHaveCounts({ compute: 1 });
    });
  });

  describe('Hydration tracking semantics for subscriptions', () => {
    it('hydration works with subscriptions', async () => {
      const stateCount = state(0);

      // Spy functions for hydration
      const hydrateSpy = vi.fn(value => value);

      // Pre-populate the store
      store.addEntry('sub-counter', 5);

      // Create a subscription with internal state
      const subCounter = subscription<number>(
        ({ get, set }) => {
          // entangle a dependency
          stateCount.get();

          // Return an object with update method to simulate changes
          return {
            update: () => {
              set(get() + stateCount.get());
            },
          };
        },
        {
          persist: {
            key: 'sub-counter',
            hydrate: hydrateSpy,
          },
          initValue: 0,
        },
      );

      const computed = reactive(() => {
        return subCounter.value;
      });

      // First access - should use hydrated value
      expect(computed).toHaveValueAndCounts(5, { compute: 1 });
      expect(subCounter).toHaveValueAndCounts(5, { compute: 1, subscribe: 1, internalSet: 0 });
      expect(hydrateSpy).toHaveBeenCalledTimes(1);

      // Reset spy for clarity
      hydrateSpy.mockClear();
      stateCount.set(1);

      // wait for the subscription to update
      await nextTick();

      // Verify the value updated but hydrate wasn't called again
      expect(subCounter.value).toBe(6);
      expect(computed).toHaveValueAndCounts(6, { compute: 2 });
      expect(hydrateSpy).not.toHaveBeenCalled();
      expect(subCounter).toHaveCounts({ subscribe: 1, update: 1 });
    });

    it('hydration is tracked and the subscription is recreated and rehydrated when the external state changes', async () => {
      const state1 = state(0);
      const state2 = state(0);

      // Spy functions for hydration
      const hydrateSpy = vi.fn(value => {
        state1.get();
        return value;
      });

      const dehydrateSpy = vi.fn(value => value);

      // Pre-populate the store
      store.addEntry('sub-counter', 5);

      // Create a subscription with internal state
      const subCounter = reactive(() => {
        state1.get();

        return subscription<number>(
          ({ get, set }) => {
            state2.get();

            // Return an object with update method to simulate changes
            return {
              update: () => {
                set(get() + state2.get());
              },
            };
          },
          {
            persist: {
              key: 'sub-counter',
              hydrate: hydrateSpy,
              dehydrate: dehydrateSpy,
            },
            initValue: 0,
          },
        );
      });

      const computed = reactive(() => subCounter().value);

      // First access - should use hydrated value
      expect(computed).toHaveValueAndCounts(5, { compute: 1 });
      expect(hydrateSpy).toHaveBeenCalledTimes(1);
      expect(dehydrateSpy).not.toHaveBeenCalled();

      // Reset spy for clarity
      hydrateSpy.mockClear();
      state2.set(1);

      await nextTick();

      // Verify the value updated but hydrate wasn't called again
      expect(computed).toHaveSignalValue(6);
      expect(hydrateSpy).not.toHaveBeenCalled();
      expect(dehydrateSpy).toHaveBeenCalledWith(6);

      dehydrateSpy.mockClear();
      state1.set(1);

      // Verify the value rehydrated
      expect(computed).toHaveSignalValue(6);
      expect(hydrateSpy).toHaveBeenCalledTimes(1);
      expect(dehydrateSpy).not.toHaveBeenCalled();
    });
  });

  describe('Nested hydration', () => {
    it('hydrated functions can use other hydrated functions', () => {
      // Create spy functions for both hydrations
      const hydrateSpyA = vi.fn(value => value * 2);
      const hydrateSpyB = vi.fn((value, factor) => {
        // Use the first hydrated function inside this one
        return reactiveA() * factor;
      });

      // Pre-populate the store
      store.addEntry(storageKey('base-value', []), 5);
      store.addEntry(storageKey('derived-value', [3]), 30);

      // Create the first hydrated reactive function
      const reactiveA = reactive(() => 10, {
        persist: {
          key: 'base-value',
          hydrate: hydrateSpyA,
        },
      });

      // Create a second reactive function that uses the first one
      const reactiveB = reactive(
        (factor: number) => {
          return reactiveA() * factor;
        },
        {
          persist: {
            key: 'derived-value',
            hydrate: hydrateSpyB,
          },
        },
      );

      // Access the nested function
      const value = reactiveB.withParams(3)();

      // Verify both hydrate functions were called exactly once
      expect(value).toBe(30);
      expect(hydrateSpyA).toHaveBeenCalledTimes(1);
      expect(hydrateSpyB).toHaveBeenCalledTimes(1);

      // Verify reactiveA was only computed once during hydration
      expect(reactiveA).toHaveCounts({ compute: 0, get: 1 });
      expect(reactiveB.withParams(3)).toHaveCounts({ compute: 0 });
    });

    it('changes to inner hydrated function propagate to outer function', () => {
      // Create a state signal that the inner function will depend on
      const innerState = state(5);

      // Create spy functions for both hydrations
      const hydrateSpyA = vi.fn(() => innerState.get() * 2);
      const hydrateSpyB = vi.fn(value => reactiveA() + 1);

      // Pre-populate the store
      store.addEntry(storageKey('inner-value', []), 10);
      store.addEntry(storageKey('outer-value', []), 11);

      // Create the first hydrated reactive function that depends on state
      const reactiveA = reactive(() => innerState.get() * 2, {
        persist: {
          key: 'inner-value',
          hydrate: hydrateSpyA,
        },
      });

      // Create a second reactive function that uses the first one
      const reactiveB = reactive(() => reactiveA() + 1, {
        persist: {
          key: 'outer-value',
          hydrate: hydrateSpyB,
        },
      });

      // First access - both should be hydrated
      expect(reactiveA()).toBe(10); // 5 * 2
      expect(reactiveB()).toBe(11); // 10 + 1

      // Reset spies for clarity
      hydrateSpyA.mockClear();
      hydrateSpyB.mockClear();

      // Update the inner state
      innerState.set(7);
      runBatch(() => {});

      // Verify both functions updated but hydrate wasn't called again
      expect(reactiveA()).toBe(14); // 7 * 2
      expect(reactiveB()).toBe(15); // 14 + 1
      expect(hydrateSpyA).not.toHaveBeenCalled();
      expect(hydrateSpyB).not.toHaveBeenCalled();
    });
  });

  describe('Dehydrate function tracking', () => {
    it('dehydrate function should not be tracked as a reactive dependency', async () => {
      // This test is expected to fail until the bug is fixed

      // Create a state signal
      const stateForDehydrate = state(10);

      // Create a reactive function with a dehydrate that uses the state
      const reactiveWithDehydrate = reactive(() => 42, {
        persist: {
          key: 'untracked-dehydrate',
          dehydrate: value => {
            // This should NOT establish a dependency
            return { value, stateValue: stateForDehydrate.get() };
          },
        },
      });

      const computed1 = reactive(() => reactiveWithDehydrate());
      const computed2 = reactive(() => computed1());

      // First access to initialize
      expect(computed2).toHaveValueAndCounts(42, { compute: 1 });
      expect(computed1).toHaveValueAndCounts(42, { compute: 1 });
      expect(reactiveWithDehydrate).toHaveValueAndCounts(42, { compute: 1 });

      // Update the state used in dehydrate
      stateForDehydrate.set(20);

      await nextTick();

      // Access again
      expect(computed2).toHaveValueAndCounts(42, { compute: 1 });
      expect(computed1).toHaveValueAndCounts(42, { compute: 1 });
      expect(reactiveWithDehydrate).toHaveValueAndCounts(42, { compute: 1 });
    });
  });

  describe('Edge cases', () => {
    it('hydrate function is not called for subsequent accesses of the same signal', () => {
      // Spy function for hydration
      const hydrateSpy = vi.fn(value => value);

      // Pre-populate the store
      store.addEntry(storageKey('repeated-access', []), 42);

      // Create a reactive function with persistence
      const repeatedAccess = reactive(() => 100, {
        persist: {
          key: 'repeated-access',
          hydrate: hydrateSpy,
        },
      });

      // Access multiple times
      repeatedAccess();
      repeatedAccess();
      repeatedAccess();

      // Verify hydrate was only called once
      expect(hydrateSpy).toHaveBeenCalledTimes(1);
    });

    it('hydrate is not called if persistence store is unavailable', () => {
      // Spy function for hydration
      const hydrateSpy = vi.fn(value => value);

      // Create a reactive function with persistence
      const noStoreSig = reactive(() => 42, {
        persist: {
          key: 'no-store',
          hydrate: hydrateSpy,
        },
      });

      // Remove the persistence store
      setConfig({
        persistenceStore: undefined,
      });

      // Access the value
      const value = noStoreSig();

      // Verify compute was called and hydrate was not
      expect(value).toBe(42);
      expect(hydrateSpy).not.toHaveBeenCalled();
      expect(noStoreSig).toHaveCounts({ compute: 1 });
    });

    it('persist is only called when value actually changes', () => {
      // Create a state to control the value
      const controlState = state(1);

      // Spy functions for hydration
      const dehydrateSpy = vi.fn(value => value);

      // Create a reactive function with persistence
      const changingValue = reactive(() => controlState.get() % 2, {
        persist: {
          key: 'changing-value',
          dehydrate: dehydrateSpy,
        },
      });

      // First access
      changingValue();
      expect(dehydrateSpy).toHaveBeenCalledTimes(1);
      expect(store.set).toHaveBeenCalledTimes(1);

      // Reset counters
      dehydrateSpy.mockClear();
      store.set.mockClear();

      // Update state to 2 - value changes to 0
      controlState.set(2);
      runBatch(() => {});
      changingValue();
      expect(dehydrateSpy).toHaveBeenCalledTimes(1);
      expect(store.set).toHaveBeenCalledTimes(1);

      // Reset counters
      dehydrateSpy.mockClear();
      store.set.mockClear();

      // Update state to 4 - value remains 0
      controlState.set(4);
      runBatch(() => {});
      changingValue();

      // Since the value didn't change, dehydrate should not be called
      expect(dehydrateSpy).not.toHaveBeenCalled();
      expect(store.set).not.toHaveBeenCalled();
    });
  });
});
type TestClosure = (
  name: string,
  fn?: jest.ProvidesCallback,
  timeout?: number
) => void;

/**
 * A test util which serves as a wrapper around test closure.
 * Runs test provided if `MutationObserver` exists in global object
 * and skips test provided (runs `xtext`) if no `MutationObserver` found
 * @param testArgs paramaters of `test` function
 */
export const testWithMutationObserver: TestClosure = (...args) => {
  if (global.MutationObserver) {
    test(...args);
  } else {
    xtest(...args);
  }
};
import {
  testAllInputsDisabled,
  testAllInputsHasError,
  testAllInputsIsBusy,
  testAllInputsPlaceholder,
} from '../support/reusable-test';

describe('Search Input', function() {
  beforeEach(() => {
    cy.visit('');
    cy.loadStory('inputs-search-input--workshop');
    cy.activateKnobsTab();
  });

  it('Placeholder', function() {
    const text = 'Test Placeholder attribute';
    testAllInputsPlaceholder(2, text);
  });

  it('Is Busy', function() {
    // Verify 2 inputs displayed
    testAllInputsIsBusy(2);
  });

  it('Has Error', function() {
    testAllInputsHasError(2);
  });

  it('Disabled', function() {
    testAllInputsDisabled(2);
  });
});
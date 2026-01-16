import { TodoCategory } from 'src/app/todos/todo';

export class TodoListPage {
  navigateTo() {
    return cy.visit('/todos');
  }

  getUrl() {
    return cy.url();
  }

  /**
   * Gets the title of the app when visiting the `/users` page.
   *
   * @returns the value of the element with the ID `.user-list-title`
   */
  getTodoTitle() {
    return cy.get('.todo-list-title');
  }

  /**
   * Get all the `app-user-card` DOM elements. This will be
   * empty if we're using the list view of the users.
   *
   * @returns an iterable (`Cypress.Chainable`) containing all
   *   the `app-user-card` DOM elements.
   */
  //getUserCards() {
    //return cy.get('.user-cards-container app-user-card');
  //}

  /**
   * Get all the `.user-list-item` DOM elements. This will
   * be empty if we're using the card view of the users.
   *
   * @returns an iterable (`Cypress.Chainable`) containing all
   *   the `.user-list-item` DOM elements.
   */
  getTodoListItems() {
    return cy.get('.todo-nav-list .todo-list-item');
  }

  /**
   * Clicks the "view profile" button for the given user card.
   * Requires being in the "card" view.
   *
   * @param card The user card
   */
  //clickViewProfile(card: Cypress.Chainable<JQuery<HTMLElement>>) {
  //  return card.find<HTMLButtonElement>('[data-test=viewProfileButton]').click();
  //}

  /**
   * Change the view of users.
   *
   * @param viewType Which view type to change to: "card" or "list".
   */
  //changeView(viewType: 'card' | 'list') {
  //  return cy.get(`[data-test=viewTypeRadio] mat-radio-button[value="${viewType}"]`).click();
  //}

  /**
   * Selects a role to filter in the "Role" selector.
   *
   * @param value The role *value* to select, this is what's found in the mat-option "value" attribute.
   */
  selectCategory(value: TodoCategory) {
    // Find and click the drop down
    return cy.get('[data-test=todoCategorySelect]').click()
      // Select and click the desired value from the resulting menu
      .get(`mat-option[value="${value}"]`).click();
      // NOTE: THIS CHAINING MIGHT BE FRAGILE (due to a 'click' followed by a 'get')
  }

  selectStatus(value: boolean) {
    // Find and click the drop down
    return cy.get('[data-test=todoCategorySelect]').click()
      // Select and click the desired value from the resulting menu
      .get(`mat-option[value="${value}"]`).click();
      // NOTE: THIS CHAINING MIGHT BE FRAGILE (due to a 'click' followed by a 'get')
  }
}
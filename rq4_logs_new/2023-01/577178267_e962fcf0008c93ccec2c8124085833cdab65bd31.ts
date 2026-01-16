describe('creating reviews', () => {
  const username = `reviews-test-${Math.random()
    .toString(36)
    .substring(2, 10)}`;
  const password = `${Math.random().toString(36)}`;

  before(() => {
    cy.signUpCustom(username, password);
    cy.signOut();
  });

  beforeEach(() => {
    cy.signInCustom(username, password);
    cy.visit('/');
  });

  it('should create a review, edit it, and then delete it', () => {
    const userBookInformation = {
      book_id: 'WwN4BgAAQBAJ',
      status: 'Completed',
      score: '8',
      firstReview:
        'This book was great! I read this book as a kid and loved it. I loved the part where the spider saves the pig. ' +
        'I would recommend this book to anyone who likes to read.',
      secondReview: 'I really enjoyed this book :)',
    };

    const { book_id, status, score, firstReview, secondReview } =
      userBookInformation;

    cy.get('[data-cy="header-search"]').click();
    cy.url().should('eq', `${Cypress.config().baseUrl}search/book`);
    cy.get('[data-cy="search-input-book-title"]').type("Charlotte's Web");
    cy.get('[data-cy="search-book-results"]')
      .contains("Charlotte's Web")
      .click();

    // create the review
    cy.url().should('eq', `${Cypress.config().baseUrl}book/${book_id}`);
    cy.get('[data-cy="create-review-button"]').should('not.exist');
    cy.get(`[data-cy="review-${username}-${book_id}"]`).should('not.exist');
    cy.get('[data-cy="add-to-list-button"]').click();
    cy.get('[data-cy="list-editor-status"]').select(status);
    // calls invoke because .clear() doesn't work
    cy.get('[data-cy="list-editor-score"]').invoke('val', '').type(score);
    cy.get('[data-cy="list-editor-save-button"]').click();

    cy.get('[data-cy="create-review-button"]')
      .contains('Create a Review')
      .click();
    cy.get('[data-cy="review-text-area"]').type(firstReview);
    cy.get('[data-cy="review-submit-button"]').click();
    cy.get(`[data-cy="review-${username}-${book_id}-text"]`).contains(
      firstReview
    );

    // edit the review
    cy.get('[data-cy="create-review-button"]')
      .contains('Edit Your Review')
      .click();
    cy.get('[data-cy="review-text-area"]').should('have.value', firstReview);
    cy.get('[data-cy="review-text-area"]').clear().type(secondReview);
    cy.get('[data-cy="review-submit-button"]').click();
    cy.get(`[data-cy="review-${username}-${book_id}-text"]`).contains(
      secondReview
    );
    cy.get('[data-cy="create-review-button"]').contains('Edit Your Review');

    // delete the review
    cy.get('[data-cy="review-delete-review-button"]')
      .contains('Delete Your Review')
      .click();
    cy.get(`[data-cy="review-${username}-${book_id}-text"]`).should(
      'not.exist'
    );
    cy.get('[data-cy="create-review-button"]').contains('Create a Review');
    cy.reload();
    cy.get(`[data-cy="review-${username}-${book_id}-text"]`).should(
      'not.exist'
    );
    cy.get('[data-cy="create-review-button"]').contains('Create a Review');
  });
});

export {};
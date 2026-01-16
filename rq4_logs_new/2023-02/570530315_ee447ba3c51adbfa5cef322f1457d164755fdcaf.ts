const dragAndDrop = (index) => {
  cy.get('[data-cy="ingredient"]').eq(index).trigger('dragstart');
  cy.get('[data-cy="drop-target"]').trigger('drop');
};

describe('template spec', () => {
  beforeEach(function () {
		cy.viewport(1300, 800);
		cy.visit('http://localhost:3000');
	})

  it('should open and close ingredient modals', function() {
    cy.get('[data-cy="ingredient"]').eq(0).click();
    cy.get('[data-cy="modal-close-button"]').click();
  });

	it('should work correctly drag and drop', function () {
		dragAndDrop(0);
		cy.get('[data-cy="top-bun"]')
			.children().should(($children) => {
				expect($children).to.have.length(1);
			});
		cy.get('[data-cy="bottom-bun"]')
			.children().should(($children) => {
				expect($children).to.have.length(1);
			});

		dragAndDrop(1)
		cy.get('[data-cy="top-bun"]')
			.children().should(($children) => {
				expect($children).to.have.length(1);
			});
		cy.get('[data-cy="bottom-bun"]')
			.children().should(($children) => {
				expect($children).to.have.length(1);
			});

		dragAndDrop(2)
		dragAndDrop(3)
		dragAndDrop(4)
		cy.get('[data-cy="choices"]')
			.children().should(($children) => {
				expect($children).to.have.length(3);
			});
	});

  it('should click button and login to create order', function() {
		dragAndDrop(0);
		dragAndDrop(2);
		dragAndDrop(3);
    cy.get('button').contains('Оформить заказ').click();
    cy.get('.input').eq(0).type('mail12345@mail.ru');
    cy.get('.input').eq(1).type('123');
    cy.get('button').contains('Войти').click();
    cy.get('button').contains('Оформить заказ').click();
    cy.get('[data-cy="modal-close-button"]').click();
  });
})
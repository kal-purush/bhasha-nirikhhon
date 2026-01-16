import { autoWired } from '@utils';
import { inject, injectable } from 'inversify';
import { BackofficePage, ActionEnum } from '@pages/backoffice';
import { CustomerIndexRepository } from './customer-index-repository';
import Chainable = Cypress.Chainable;

@injectable()
@autoWired
export class CustomerIndexPage extends BackofficePage {
  @inject(CustomerIndexRepository) private repository: CustomerIndexRepository;

  protected PAGE_URL = '/customer';

  update = (params: UpdateParams): void => {
    this.find({
      searchQuery: params.searchQuery,
      tableUrl: '/customer/index/table**',
    }).then(($userRow) => {
      if (params.action === ActionEnum.removeMultiFactorAuthentication) {
        cy.wrap($userRow)
          .find(this.getRemoveMultiFactorAuthenticationButtonSelector())
          .should('exist')
          .click({ force: true });
      }
    });
  };

  findCustomer(params: FindParams): Chainable {
    return this.find({ searchQuery: params.searchQuery, tableUrl: '/customer/index/table**' });
  }

  assertRemoveMultiFactorAuthenticationButtonDoesNotExist = (params: FindParams): void => {
    this.findCustomer(params).should('not.contain', this.getRemoveMultiFactorAuthenticationButtonSelector());
  };

  getRemoveMultiFactorAuthenticationButtonSelector = (): string =>
    this.repository.getRemoveMultiFactorAuthenticationButtonSelector();
}

interface UpdateParams {
  action: ActionEnum;
  searchQuery: string;
}

interface FindParams {
  searchQuery: string;
}
import { clickTravelPolicyCtaButton } from '.';
import { TravelPolicy } from '../../../../../models';

export const cancelDialogAndConfirm = (travelPolicyDetails: TravelPolicy): void => {
  clickTravelPolicyCtaButton(travelPolicyDetails.sharedDetails.modifiedName, 'delete');

  cy.get('.cdk-overlay-container confirmation-dialog ui-button[cancel=true]').click();

  cy.get('dib-company-management dib-travel-policy dib-expandable-item .section__header__title').should(
    'contain',
    travelPolicyDetails.sharedDetails.modifiedName
  );

  cy.get('.cdk-overlay-container confirmation-dialog').should('not.exist');
};
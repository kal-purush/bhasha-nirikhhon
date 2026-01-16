import { selectTravelerGroup } from '.';
import { ApprovalProcess } from '../../../../../models';

export const addApprovalProcessForGroupAndConfirm = (approvalProcessForGroup: ApprovalProcess): void => {
  cy.get('dib-company-management dib-approval-process ui-button[type=primary]').click();

  selectTravelerGroup(approvalProcessForGroup);

  cy.get('.cdk-overlay-container dib-approval-process-dialog .radio-button-group label')
    .contains("Don't need approval (exception from travel policy)")
    .click();
  cy.get('.cdk-overlay-container dib-approval-process-dialog ui-button[type=success]').click();

  cy.get('.cdk-overlay-container simple-snack-bar > span').should('contain', 'Approval process successfully created.');
  cy.get('dib-company-management dib-approval-process dib-approval-process-item .item__left')
    .first()
    .find('.item__content p')
    .should('contain', approvalProcessForGroup.travelersGroupName);
};
import {
  CompleteMembershipRequest,
  PackedFolderItemFactory,
} from '@graasp/sdk';

import { buildContentPagePath } from '@/config/paths';
import {
  FORBIDDEN_CONTENT_ID,
  REQUEST_MEMBERSHIP_BUTTON_ID,
} from '@/config/selectors';
import { ID_FORMAT } from '@/utils/item';

import { CURRENT_USER } from '../fixtures/members';
import { API_HOST } from '../support/env';
import { DEFAULT_POST } from '../support/utils';

const item = PackedFolderItemFactory({}, { permission: null });

describe('Membership Request', () => {
  describe('Logged out', () => {
    beforeEach(() => {
      cy.setUpApi({
        currentMember: null,
        items: [item],
      });
    });

    it('Forbidden', () => {
      cy.visit(buildContentPagePath({ rootId: item.id, itemId: item.id }));

      cy.get(`#${FORBIDDEN_CONTENT_ID}`).should('be.visible');
    });
  });

  describe('Logged in', () => {
    beforeEach(() => {
      cy.setUpApi({
        items: [item],
      });
    });

    it('Request membership', () => {
      cy.intercept(
        {
          method: DEFAULT_POST.method,
          url: new RegExp(
            `${API_HOST}/items/${ID_FORMAT}/memberships/requests$`,
          ),
        },
        ({ reply }) => {
          reply({
            member: CURRENT_USER,
            item,
            createdAt: Date.now().toString(),
          } as CompleteMembershipRequest);
        },
      ).as('request');

      cy.visit(buildContentPagePath({ rootId: item.id, itemId: item.id }));

      cy.get(`#${REQUEST_MEMBERSHIP_BUTTON_ID}`).click();

      cy.wait('@request');
    });
  });
});
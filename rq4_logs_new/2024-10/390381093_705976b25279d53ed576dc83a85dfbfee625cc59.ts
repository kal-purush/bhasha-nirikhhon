import { API_ROUTES } from '@graasp/query-client';
import {
  CompleteGuest,
  DiscriminatedItem,
  FolderItemFactory,
  GuestFactory,
  HttpMethod,
  ItemLoginSchemaFactory,
  ItemLoginSchemaType,
} from '@graasp/sdk';

import { StatusCodes } from 'http-status-codes';

import { buildAutoLoginPath, buildContentPagePath } from '@/config/paths';
import {
  AUTO_LOGIN_CONTAINER_ID,
  AUTO_LOGIN_ERROR_CONTAINER_ID,
  AUTO_LOGIN_NO_ITEM_LOGIN_ERROR_ID,
} from '@/config/selectors';

import { API_HOST, AUTH_HOST } from '../support/env';
import { expectFolderLayout } from '../support/integrationUtils';

const {
  buildPostItemLoginSignInRoute,
  buildGetItemLoginSchemaTypeRoute,
  buildGetCurrentMemberRoute,
  buildGetItemRoute,
} = API_ROUTES;

class TestHelper {
  private isLoggedIn: boolean = false;

  private pseudoMember: CompleteGuest;

  private item: DiscriminatedItem;

  private returnItemLoginSchemaType: boolean = true;

  constructor(args: {
    pseudoMember: CompleteGuest;
    item: DiscriminatedItem;
    initiallyIsLoggedIn?: boolean;
    returnItemLoginSchemaType?: boolean;
  }) {
    this.pseudoMember = JSON.parse(JSON.stringify(args.pseudoMember));
    this.item = JSON.parse(JSON.stringify(args.item));
    if (args.initiallyIsLoggedIn) {
      this.isLoggedIn = true;
    }
    if (args.returnItemLoginSchemaType === false) {
      this.returnItemLoginSchemaType = false;
    }
  }

  setupServer() {
    // current member call
    cy.intercept(
      {
        method: HttpMethod.Get,
        url: `${API_HOST}/${buildGetCurrentMemberRoute()}`,
      },
      ({ reply }) => {
        if (this.isLoggedIn) {
          return reply({ statusCode: StatusCodes.OK, body: this.pseudoMember });
        }
        return reply({ statusCode: StatusCodes.UNAUTHORIZED });
      },
    ).as('getCurrentMember');
    // allow to login
    cy.intercept(
      {
        method: HttpMethod.Post,
        url: `${API_HOST}/${buildPostItemLoginSignInRoute(this.item.id)}`,
      },
      ({ reply }) => {
        if (this.returnItemLoginSchemaType) {
          // save that the user is now logged in
          this.isLoggedIn = true;
          return reply({ statusCode: StatusCodes.NO_CONTENT });
        }
        return reply({ statusCode: StatusCodes.BAD_REQUEST });
      },
    ).as('postItemLoginSchemaType');
    cy.intercept(
      {
        method: HttpMethod.Get,
        url: `${API_HOST}/${buildGetItemLoginSchemaTypeRoute(this.item.id)}`,
      },
      ({ reply }) => {
        if (this.returnItemLoginSchemaType) {
          return reply(this.pseudoMember.itemLoginSchema.type);
        }
        return reply({ statusCode: StatusCodes.NOT_FOUND });
      },
    ).as('getItemLoginSchemaType');

    cy.intercept(
      {
        method: HttpMethod.Get,
        url: new RegExp(`${API_HOST}/${buildGetItemRoute(this.item.id)}$`),
      },
      ({ reply }) => {
        if (this.isLoggedIn) {
          reply(this.item);
        }
      },
    ).as('getItem');

    cy.intercept(
      {
        method: HttpMethod.Get,
        url: AUTH_HOST,
      },
      ({ reply }) =>
        reply({
          body: '<h1>Auth</h1>',
          headers: { 'content-type': 'text/html' },
        }),
    );
  }
}

const pseudonimizedItem = FolderItemFactory({ name: 'Pseudo Item' });
const pseudoMember = GuestFactory({
  name: '1234',
  itemLoginSchema: ItemLoginSchemaFactory({
    type: ItemLoginSchemaType.Username,
    item: pseudonimizedItem,
  }),
});

describe('Auto Login on pseudonimized item', () => {
  beforeEach(() => {
    const helper = new TestHelper({ item: pseudonimizedItem, pseudoMember });
    helper.setupServer();
  });
  it('Allows auto login on item with item login', () => {
    const search = new URLSearchParams({
      username: '1234',
      fullscreen: 'true',
    });

    const routeArgs = {
      rootId: pseudonimizedItem.id,
      itemId: pseudonimizedItem.id,
      searchParams: search.toString(),
    };
    cy.visit(buildAutoLoginPath(routeArgs));
    cy.get(`#${AUTO_LOGIN_CONTAINER_ID}`).should('be.visible');
    cy.get(`#${AUTO_LOGIN_CONTAINER_ID} [type="button"]`).click();

    // checks that the user was correctly redirected to the item page
    const { searchParams, ...pathArgs } = routeArgs;
    cy.location('pathname').should('equal', buildContentPagePath(pathArgs));
    // keep the search params
    cy.location('search').should('equal', `?${searchParams}`);
  });
  it('Missing username triggers error', () => {
    const routeArgs = {
      rootId: pseudonimizedItem.id,
      itemId: pseudonimizedItem.id,
    };
    cy.visit(buildAutoLoginPath(routeArgs));
    cy.get(`#${AUTO_LOGIN_ERROR_CONTAINER_ID}`).should('be.visible');
    cy.get(`#${AUTO_LOGIN_ERROR_CONTAINER_ID} [type="button"]`).click();

    cy.location('pathname').should('equal', '/');
  });
});

describe('Auto Login on private item', () => {
  beforeEach(() => {
    const helper = new TestHelper({
      item: pseudonimizedItem,
      pseudoMember,
      returnItemLoginSchemaType: false,
    });
    helper.setupServer();
  });
  it('Fails if itemLogin is not enabled', () => {
    const search = new URLSearchParams({
      username: '1234',
      fullscreen: 'true',
    });
    const routeArgs = {
      rootId: pseudonimizedItem.id,
      itemId: pseudonimizedItem.id,
      searchParams: search.toString(),
    };
    cy.visit(buildAutoLoginPath(routeArgs));
    cy.get(`#${AUTO_LOGIN_NO_ITEM_LOGIN_ERROR_ID}`).should('be.visible');
  });
});

describe('Auto Login with logged in user', () => {
  beforeEach(() => {
    const helper = new TestHelper({
      item: pseudonimizedItem,
      pseudoMember,
      initiallyIsLoggedIn: true,
    });
    helper.setupServer();
  });
  it('Redirects to item page', () => {
    const search = new URLSearchParams({
      username: '1234',
      fullscreen: 'true',
    });
    const routeArgs = {
      rootId: pseudonimizedItem.id,
      itemId: pseudonimizedItem.id,
      searchParams: search.toString(),
    };
    cy.visit(buildAutoLoginPath(routeArgs));
    expectFolderLayout({
      rootId: pseudonimizedItem.id,
      items: [pseudonimizedItem],
    });
  });
});
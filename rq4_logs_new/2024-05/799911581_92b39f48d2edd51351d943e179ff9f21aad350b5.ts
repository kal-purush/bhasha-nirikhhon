import { expect, Page } from '@playwright/test';

import { A11Y, HREFS, TEST_IDS } from '@/shared/core/constants';

import { ORG_TEST_IDS } from '@/orgs/core/constants';

import { getNavLocator } from './nav-helpers';

export const navigateToOrgDetails = async (page: Page, n: number) => {
  const card = page.getByTestId(ORG_TEST_IDS.ORG_CARD).nth(n);
  const uuid = await card.getAttribute('data-uuid');

  // Click on the upper right corner (ensure buttons w/in the card is not clicked)
  await card.click({ position: { x: 10, y: 10 } });

  await expect(page).toHaveURL(`/organizations/${uuid}/details`);
};

export const navigateBackToOrgListPage = async (page: Page) => {
  // Assert mobile/tablet devices are currently on details-page
  await expect(page).toHaveURL(/\/organizations\/\d+\/details/);

  // Click back button
  await page.getByTestId(TEST_IDS.DETAILS_BACK).click();

  // Assert currently on org-list page
  await expect(page).toHaveURL(HREFS.ORGS_PAGE);
};

export const navigateToOrgListPage = async (page: Page) => {
  await getNavLocator(page, A11Y.LINK.NAV.ORGS).click();
  await expect(page).toHaveURL(HREFS.ORGS_PAGE);
};
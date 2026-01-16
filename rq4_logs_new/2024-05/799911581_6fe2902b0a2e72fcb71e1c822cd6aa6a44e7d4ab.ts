import { Page } from '@playwright/test';

import { A11Y, TEST_IDS } from '@/shared/core/constants';

import { assertActiveAttribute } from './common-helpers';

const getNavLocator = (page: Page, name: string) => {
  const button = page.getByRole('button', { name: name });
  return page.getByTestId(TEST_IDS.NAV_SECTION).locator(button);
};

export const clickNavButton = async (page: Page, name: string) =>
  getNavLocator(page, name).click();

// Asserts link-name is active
export const assertNavActiveStatus = async (
  page: Page,
  name: string,
  isActive: boolean,
) => assertActiveAttribute(getNavLocator(page, name), isActive);

// Asserts link-name provided is active while the rest are inactive
export const assertNavLinksStatuses = async (
  page: Page,
  activeLinkName: string,
) => {
  const inactiveLinkNames = Object.values(A11Y.LINK.NAV).filter(
    (linkName) => linkName !== activeLinkName,
  );

  await Promise.all([
    // Assert active link
    assertNavActiveStatus(page, activeLinkName, true),
    // Assert inactive links
    ...inactiveLinkNames.map((inactiveLinkName) =>
      assertNavActiveStatus(page, inactiveLinkName, false),
    ),
  ]);
};

export const openMobileNav = async (page: Page) =>
  page.getByTestId(TEST_IDS.MOBILE_MENU).click();

export const openMobileNavFromDetails = async (page: Page) =>
  page.getByTestId(TEST_IDS.MOBILE_MENU).nth(1).click();
import {
  expect,
  SkyHostBrowser
} from '@skyux-sdk/e2e';

import {
  browser,
  by,
  element,
  protractor
} from 'protractor';

describe('Country field', () => {
  beforeEach(() => {
    SkyHostBrowser.get('visual/country-field');
    SkyHostBrowser.setWindowBreakpoint('lg');
  });

  function selectCountryField(done: DoneFn): void {
    const input = element(by.css('#screenshot-country-field-populated textarea'));
    input.value = '';
    input.click();
  }

  function clearCountryField(done: DoneFn): void {
    selectCountryField(done);

    browser.actions().doubleClick().perform();
    browser.actions().sendKeys(protractor.Key.BACK_SPACE).perform();

    browser.wait(() => {
      return browser.isElementPresent(
        element(by.css('body'))
      );
    });

    element(by.css('body')).click();
  }

  it('should match previous screenshot with no selection', (done) => {
    expect('#screenshot-country-field-empty').toMatchBaselineScreenshot(done, {
      screenshotName: 'country-field-empty'
    });
  });

  it('should match previous screenshot with no selection (xs screen)', (done) => {
    SkyHostBrowser.setWindowBreakpoint('xs');
    expect('#screenshot-country-field-empty').toMatchBaselineScreenshot(done, {
      screenshotName: 'country-field-empty-xs'
    });
  });

  it('should match previous screenshot with a selection', (done) => {
    expect('#screenshot-country-field-populated').toMatchBaselineScreenshot(done, {
      screenshotName: 'country-field-populated'
    });
  });

  it('should match previous screenshot with a selection (xs screen)', (done) => {
    SkyHostBrowser.setWindowBreakpoint('xs');
    expect('#screenshot-country-field-populated').toMatchBaselineScreenshot(done, {
      screenshotName: 'country-field-populated-xs'
    });
  });

  it('should match previous screenshot with an error', (done) => {
    clearCountryField(done);
    expect('#screenshot-country-field-populated').toMatchBaselineScreenshot(done, {
      screenshotName: 'country-field-error'
    });
  });

  it('should match previous screenshot with an error (xs screen)', (done) => {
    clearCountryField(done);
    SkyHostBrowser.setWindowBreakpoint('xs');
    expect('#screenshot-country-field-populated').toMatchBaselineScreenshot(done, {
      screenshotName: 'country-field-error-xs'
    });
  });

  it('should match previous screenshot while selected', (done) => {
    selectCountryField(done);
    expect('#screenshot-country-field-populated').toMatchBaselineScreenshot(done, {
      screenshotName: 'country-field-selected'
    });
  });

  it('should match previous screenshot while selected (xs screen)', (done) => {
    selectCountryField(done);
    SkyHostBrowser.setWindowBreakpoint('xs');
    expect('#screenshot-country-field-populated').toMatchBaselineScreenshot(done, {
      screenshotName: 'country-field-selected-xs'
    });
  });

  it('should match previous screenshot while disabled and empty', (done) => {
    element(by.css('#disable-toggle')).click();
    expect('#screenshot-country-field-empty').toMatchBaselineScreenshot(done, {
      screenshotName: 'country-field-empty-disabled'
    });
  });

  it('should match previous screenshot while disabled and empty (xs screen)', (done) => {
    element(by.css('#disable-toggle')).click();
    SkyHostBrowser.setWindowBreakpoint('xs');
    expect('#screenshot-country-field-empty').toMatchBaselineScreenshot(done, {
      screenshotName: 'country-field-empty-disabled-xs'
    });
  });

  it('should match previous screenshot while disabled and populated', (done) => {
    element(by.css('#disable-toggle')).click();
    expect('#screenshot-country-field-populated').toMatchBaselineScreenshot(done, {
      screenshotName: 'country-field-populated-disabled'
    });
  });

  it('should match previous screenshot while disabled and populated (xs screen)', (done) => {
    element(by.css('#disable-toggle')).click();
    SkyHostBrowser.setWindowBreakpoint('xs');
    expect('#screenshot-country-field-populated').toMatchBaselineScreenshot(done, {
      screenshotName: 'country-field-populated-disabled-xs'
    });
  });
});
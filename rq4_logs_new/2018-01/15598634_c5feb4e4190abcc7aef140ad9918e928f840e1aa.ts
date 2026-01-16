import { expect } from 'chai';
import { screenshotElement } from './utilities';

describe('test suite', () => {
  it('verifies the about page', (done) => {
    browser.url('/about');
    expect(browser.getTitle()).to.equal('Elia Karagiannis');
    screenshotElement('about-page-content', browser.element('main'));
  });
});
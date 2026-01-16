/// <reference path="../typings/main.d.ts" />

import { WorkshopPage } from './app.po';

describe('workshop App', function() {
  let page: WorkshopPage;

  beforeEach(() => {
    page = new WorkshopPage();
  })

  it('should display message saying app works', () => {
    page.navigateTo()
    expect(page.getParagraphText()).toEqual('workshop Works!');
  });
});
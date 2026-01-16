import { NgAdminsystemPage } from './app.po';

describe('ng-adminsystem App', () => {
  let page: NgAdminsystemPage;

  beforeEach(() => {
    page = new NgAdminsystemPage();
  });

  it('should display welcome message', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('Welcome to app!');
  });
});
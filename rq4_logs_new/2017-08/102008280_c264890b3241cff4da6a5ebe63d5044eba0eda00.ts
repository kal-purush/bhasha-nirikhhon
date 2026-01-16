import { AgGridCliPage } from './app.po';

describe('ag-grid-cli App', () => {
  let page: AgGridCliPage;

  beforeEach(() => {
    page = new AgGridCliPage();
  });

  it('should display message saying "Simple ag-Grid Angular Example"', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('Simple ag-Grid Angular Example');
  });
});
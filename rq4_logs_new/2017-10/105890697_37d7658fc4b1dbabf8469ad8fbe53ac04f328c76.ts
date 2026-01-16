describe('mean App', () => {

  beforeEach(() => {
    browser.get("http://localhost:4200/");
  });

  it('should display welcome message', () => {
    expect(browser.getTitle()).toEqual('Your MEAN Stack App');
  });
});
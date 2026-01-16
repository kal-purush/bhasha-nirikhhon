import { JsonSchemaFormTestPage } from './app.po';

describe('json-schema-form-test App', () => {
  let page: JsonSchemaFormTestPage;

  beforeEach(() => {
    page = new JsonSchemaFormTestPage();
  });

  it('should display welcome message', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('Welcome to app!');
  });
});
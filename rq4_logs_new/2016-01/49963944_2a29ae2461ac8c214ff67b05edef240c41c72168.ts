
describe('Searh Todo', () => {

  beforeEach(function() {
    browser.get('/');
  });

  it('should successfully search todo', function() {
    let search = element(by.id('search_todo'));
    search.sendKeys('one');
    // search.sendKeys(protractor.Key.ENTER);

    let todos = element.all(by.css('.collection-item'));

    todos.map((element) => {return element.getText();})
         .then((txts) => expect(txts[0]).toBe('one'));

    // expect(todos[0].getText()).toBe('one');

  });

});
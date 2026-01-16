import ICucumber = Cube.ICucumber;
var exportedModule = function () {

    var cucumber:ICucumber = this;

    cucumber.Given(/^I am on the application page$/, function () {
        return browser.get("http://localhost:8080/");
    });

    cucumber.When(/^I press button$/, function () {
        this.title = browser.getTitle();
    });

    cucumber.Then(/^I should see answer "(.*)"$/, function (expectedTitle:string) {
        var title = this.title;
        return this.expect(title).to.eventually.equal(expectedTitle);
    })
};
export = exportedModule;
import { SearchPage }        from './search.po';
import { browser, element, by } from 'protractor';

describe("Search feature", () => {
  let page: SearchPage;
  let allPersons: any = {};

  beforeEach(() => {
    page = new SearchPage();
    page.navigateTo();
    allPersons = element.all(by.css("md-list-item"));
  })

  it('displays Search in the h2 tag', () => {
    expect(element(by.css("h2")).getText()).toEqual('Search');
  });

  it('displays all persons if home button is clicked', () => {
    element(by.css("button#show-all")).click();

    expect(allPersons.count()).toBe(3);
    expect(page.getElemContent("name-1")).toBe("Bob Smith");
    expect(page.getElemContent("name-2")).toBe("Jim Smith");
    expect(page.getElemContent("ad-street-2")).toBe("321 North Kings Highway");
    expect(page.getElemContent("ad-city-state-zip-2")).toBe("Myrtle Beach, SC 29577");
    expect(page.getElemContent("name-3")).toBe("Tom Smith");
  })

  it("displays all persons if '*' is searched", () => {
    element(by.css("input#query")).sendKeys("*");
    element(by.css("button#search")).click();

    expect(allPersons.count()).toBe(3);
    expect(page.getElemContent("name-phone-1")).toBe("Bob Smith (843-555-1234)");
    expect(page.getElemContent("ad-street-1")).toBe("123 North Kings Highway");
    expect(page.getElemContent("ad-city-state-zip-1")).toBe("Myrtle Beach, SC 29577");
    expect(page.getElemContent("ad-street-2")).toBe("321 North Kings Highway");
    expect(page.getElemContent("ad-street-3")).toBe("222 North Kings Highway");
  })

  it("displays all persons if no search term is added during search", () => {
    element(by.css("input#query")).sendKeys("");
    element(by.css("button#search")).click();

    expect(allPersons.count()).toBe(3);
    expect(page.getElemContent("name-phone-1")).toBe("Bob Smith (843-555-1234)");
    expect(page.getElemContent("name-phone-2")).toBe("Jim Smith (843-555-2345)");
    expect(page.getElemContent("name-phone-3")).toBe("Tom Smith (843-555-3456)");
    expect(page.getElemContent("ad-street-3")).toBe("222 North Kings Highway");
    expect(page.getElemContent("ad-city-state-zip-3")).toBe("Myrtle Beach, SC 29577");
  })

  it("finds the person if its data is searched", () => {
    element(by.css("input#query")).sendKeys("bob");
    element(by.css("button#search")).click();

    expect(allPersons.count()).toBe(1);
    expect(page.getElemContent("name-phone-1")).toBeDefined();    
  });

  it("informs user if search was not successfull", () => {
    element(by.css("input#query")).sendKeys("ccc");
    element(by.css("button#search")).click();

    expect(allPersons.count()).toBe(0);
    expect(page.getElemContent("no-result")).toBeDefined();
    expect(page.getElemContent("no-result")).toBe("No results found");
  })

  it("opens the edit page if one of the persons name clicked", () => {
    element(by.css("button#show-all")).click();
    element(by.css("a#name-3")).click();

    expect(browser.getCurrentUrl()).toBe("http://localhost:4200/edit/3");
  })

})
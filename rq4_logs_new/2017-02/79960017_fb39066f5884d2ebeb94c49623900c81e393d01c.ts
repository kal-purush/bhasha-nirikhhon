import { EditPage }             from './edit.po';
import { browser, element, by } from 'protractor';

describe("Edit feature", () => {
  let page: EditPage;

  beforeEach(() => {
    page = new EditPage();
    page.navigateTo("/edit/2");
  })

  it("informs user if invalid id is added to the url", () => {
    page.navigateTo("/edit/89");

    expect(page.getElemContent("no-person-found")).toBeDefined();
    expect(page.getElemContent("no-person-found")).toBe("No person found.");
  });

  it("show the person's name and id in a header", () => {
    expect(element(by.css("h3")).getText()).toBe("Jim Smith (id: 2)");
  })

  it("shows the name of the person in an input field", () => {
    expect(element(by.css("input#name")).getAttribute('ng-reflect-model')).toBe("Jim Smith");
  })

  it("shows the phone number of the person in an input field", () => {
    expect(element(by.css("input#phone")).getAttribute('ng-reflect-model')).toBe("843-555-2345");
  })

  it("shows the street info of the person's address in an input field", () => {
    expect(element(by.css("input#street")).getAttribute('ng-reflect-model')).toBe("321 North Kings Highway");
  })

  it("shows the city of the person's address in an input field", () => {
    expect(element(by.css("input#city")).getAttribute('ng-reflect-model')).toBe("Myrtle Beach");
  })

  it("shows the state of the person's address in an input field", () => {
    expect(element(by.css("input#state")).getAttribute('ng-reflect-model')).toBe("SC");
  })

  it("shows the zip of the person's address in an input field", () => {
    expect(element(by.css("input#zip")).getAttribute('ng-reflect-model')).toBe("29577");
  })

  it("goes back to search view if 'Back to search' button clicked, with no change", () => {
    element(by.id("back-to-search")).click();
    expect(browser.getCurrentUrl()).toBe("http://localhost:4200/search");

    element(by.css("input#query")).sendKeys("Jim Smith");
    element(by.css("button#search")).click();
    expect(element.all(by.css("md-list-item")).count()).toBe(1);
    expect(page.getElemContent("name-phone-2")).toBe("Jim Smith (843-555-2345)");
    expect(page.getElemContent("ad-street-2")).toBe("321 North Kings Highway");
    expect(page.getElemContent("ad-city-state-zip-2")).toBe("Myrtle Beach, SC 29577");
  });

  it("redirects user to search view if save button clicked", () => {
    element(by.id("save-person")).click();
    expect(browser.getCurrentUrl()).toBe("http://localhost:4200/search");
  })

  it("saves person's data if attribute(s) changed and save button clicked", () => {
    element.all(by.css("input")).clear();
    element(by.css("input#name")).sendKeys("James Smith");
    element(by.css("input#phone")).sendKeys("123456");
    element(by.css("input#street")).sendKeys("test street");
    element(by.css("input#city")).sendKeys("test city");
    element(by.css("input#state")).sendKeys("NC");
    element(by.css("input#zip")).sendKeys("54321");
    element(by.id("save-person")).click();

    element(by.css("input#query")).sendKeys("Jim Smith");
    element(by.css("button#search")).click();
    expect(element.all(by.css("md-list-item")).count()).toBe(0);

    element(by.css("input#query")).clear();
    element(by.css("input#query")).sendKeys("James Smith");
    element(by.css("button#search")).click();
    expect(element.all(by.css("md-list-item")).count()).toBe(1);
    expect(page.getElemContent("name-phone-2")).toBe("James Smith (123456)");
    expect(page.getElemContent("ad-street-2")).toBe("test street");
    expect(page.getElemContent("ad-city-state-zip-2")).toBe("test city, NC 54321");
  })
});
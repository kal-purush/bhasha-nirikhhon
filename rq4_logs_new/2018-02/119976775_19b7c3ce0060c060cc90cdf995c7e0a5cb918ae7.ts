import * as protractor from 'protractor';

export class ShakeCore {
  constructor(
    public browser = protractor.browser,
    public defaultWaitToAppear = 20000,
    public ec = protractor.ExpectedConditions
  ) {
  }


  public getElement = async (cssLocator: string, waitToAppear: boolean = true) => {
    const elementFound = await protractor.$(cssLocator);
    if (waitToAppear) {
      await this.toAppear(elementFound);
    }
    return elementFound;
  }

  public getElements = async (cssLocator: string, waitToAllAppear: boolean = true) => {
    let elementsFound = [];
    await this.browser.wait(
      async () => {
        elementsFound = await protractor.$$(cssLocator);
        return (elementsFound.length > 0);
      },
      this.defaultWaitToAppear, `No elements found: By(css selector: '${cssLocator}')`);

    if (waitToAllAppear) {
      for (const element of elementsFound) {
        await this.toAppear(element);
      }
    }
    return elementsFound;
  }

  public toAppear = async (elementToFind: protractor.ElementFinder): Promise<protractor.ElementHelper> => {
    await elementToFind.browser_.wait(
      this.ec.visibilityOf(elementToFind),
      this.defaultWaitToAppear,
      `No element found: By(css selector: '${elementToFind.elementArrayFinder_.locator_.value}')`,
    );
    return elementToFind;
  }
}
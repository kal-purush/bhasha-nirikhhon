import { Client, Element, RawResult } from "webdriverio";

import { BasePage } from "./base.page";

class XpathPage extends BasePage {

    public getGrid(gridNumber: number): Client<RawResult<Element>> & RawResult<Element> {
        return browser.element(`.mx-name-grid${gridNumber}`);
    }

    public getGridRow(gridNumber: number, rowNumber: number): Client<RawResult<Element>> & RawResult<Element> {
        return browser.element(`.mx-name-grid${gridNumber} .mx-name-index-${rowNumber}`);
    }

    public open(): void {
        browser.url("/p/xpath");
    }
}

const xpathPage = new XpathPage();
export default xpathPage;
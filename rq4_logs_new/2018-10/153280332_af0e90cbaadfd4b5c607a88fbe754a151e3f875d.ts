import { browser, $, ElementFinder } from 'protractor';

export class IFramePage {
  iFrame: ElementFinder;

  constructor() {
    this.iFrame = $('#IF1');
  }

  public async goToWebsite() {
    await browser.get('http://toolsqa.com/iframe-practice-page/');
  }

  public async getIFrameHeight(): Promise<number> {
    const strHeight = await this.iFrame.getAttribute('height');
    return +strHeight;
  }

  public async modifyIFrameHeight(height: number): Promise<void> {
    await browser.executeScript(
      `document.querySelector("#IF1").setAttribute('height', ${height});`
    );
  }
}
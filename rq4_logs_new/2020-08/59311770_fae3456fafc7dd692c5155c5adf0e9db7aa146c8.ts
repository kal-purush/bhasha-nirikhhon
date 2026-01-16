/**
@license
*/

import { Page, Browser, Request } from 'puppeteer';
const puppeteer = require('puppeteer');
const hf = require('../../helpers/hermetic-fonts');
const { config } = require('./server/test-server');

export interface PageOpenParams {
  route: string;
}

export interface PageOptions {
  scenarioName?: string;
}

export class PageObject {
  private _browser?: Browser;
  private _page?: Page;
  protected readonly scenarioName: string;

  constructor(options: PageOptions = {}) {
    this.scenarioName = options.scenarioName || '';
  }

  async init() {
    const browser = this._browser = await puppeteer.launch({ args: ['--disable-gpu', '--font-render-hinting=none'] });

    const page = this._page = await browser.newPage();

    page.on('console', (msg) => console.log('PAGE LOG:', msg.text()));

    page.on('requestfailed', (request: Request) => {
      console.log('PAGE REQUEST FAIL: [' + request.url() + '] ' + request.failure()!.errorText);
    });

    page.setRequestInterception(true);
    page.on('request', async (request: Request) => {
      const fontResponse = hf.serveHermeticFont(request, config.dataDir);
      if (fontResponse) {
        request.respond(fontResponse);
      } else {
        request.continue();
      }
    });

    await page.emulateTimezone('America/Toronto');
  }

  async close() {
    await this._browser?.close();
  }

  async open() {
    if (!this._page) {
      throw new Error('Page not initialized. Did you call init()?');
    }
    const params = this.openParams;

    const testFlagSeparator = (params.route && params.route.includes('?')) ? '&' : '?';
    await this._page.goto(`${config.appUrl}/${params.route}${testFlagSeparator}test_data`);
  }

  // To be overridden.
  protected get openParams(): PageOpenParams {
    throw new Error('Method not implemented.');
  }

  async screenshot(): Promise<string> {
    throw new Error("Method not implemented.");
  }

  screenshotName(): string {
    throw new Error("Method not implemented.");
  }


}
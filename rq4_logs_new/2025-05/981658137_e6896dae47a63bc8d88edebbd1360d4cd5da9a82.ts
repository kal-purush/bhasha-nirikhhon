import { describe, it, expect, vi, beforeAll } from "vitest";
import puppeteer from "puppeteer";
import { PuppeteerAgent } from "@midscene/web/puppeteer";
import "dotenv/config"; // read environment variables from .env file

vi.setConfig({
  testTimeout: 240 * 10000,
});

const pageUrl = "https://saicoksasit-saicoksa-sandbox.insuremo.com/ri/";
describe("Test reinsurance1", () => {
  let agent: PuppeteerAgent;

  beforeAll(async () => {
    const browser = await puppeteer.launch({
      headless: true,
      args: ["--no-sandbox", "--disable-setuid-sandbox"],
    });
    const page = await browser.newPage();
    await page.goto(pageUrl);
    await page.waitForNetworkIdle();
    agent = new PuppeteerAgent(page, { cacheId: "reinsurance1" });

    return () => {
      browser.close();
    };
  });

  it("ai todo", async () => {
    await agent.aiInput(
      "saicoadmin", "Username输入框"
    );
    await agent.aiInput(
      "Saicoksa123456#", "Password输入框"
    );
    await agent.aiTap(
      "点击Sign In按钮"
    );
    await agent.aiTap(
      "点击Query按钮"
    );
    await agent.aiTap(
      "点击Search按钮"
    );
    await agent.aiTap(
      "点击Quotation Number下面的第一个Hyperlink"
    );

    const dataA = await agent.aiQuery({
      riskunitIDs: '表格的Risk Unit Id的值,string[]'
    });
    console.log(dataA);
    await agent.aiTap('点击Risk Unit Id = ' + dataA.riskunitIDs[0] + '的Hyperlink');
  
    // 示例：在新页面上执行操作
    // const items = await aiQuery(
    //   "Quotation No."
    // );
    const dataB = await agent.aiQuery({
      riskunitID: 'Details下面的Risk Unit Id的值,string'
    });
    console.log(dataB);
    expect(dataA.riskunitIDs[0]).toEqual(dataB.riskunitID);
  });
});
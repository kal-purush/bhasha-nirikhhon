const puppeteer = require('puppeteer');

let browser, page;

describe('Home', () => {
    beforeAll(async () => {
        browser = await puppeteer.launch();
        page = await browser.newPage();
        await page.goto('http://localhost:8080');
    });

    it('should be titled "Hello react"', async () => {
        await expect(page.title()).resolves.toMatch('Hello React!');
    });

    it('Testing the product diet filtering', async () => {
        expect.assertions(13);

        let buttons = await page.$x("//div[@class='products']//button");
        expect(buttons.length).toBe(91);

        // Filtering by vegan products.
        let dietButton = await page.$x("//div[@class='diets']//button[@class='btn vegan']");
        await dietButton[0].click();

        // Making sure we have selected buttons effect.
        expect(await page.$x("//div[@class='diets']//button[@class='btn vegan selected']")).not.toBe(undefined);
        expect(await page.$x("//div[@class='diets']//button[@class='btn carnivore selected']").length).toBe(undefined);
        expect(await page.$x("//div[@class='diets']//button[@class='btn vegetarian selected']").length).toBe(undefined);

        buttons = await page.$x("//div[@class='products']//button");
        expect(buttons.length).toBe(53);

        // Filtering by vegan products.
        dietButton = await page.$x("//div[@class='diets']//button[@class='btn vegetarian']");

        await dietButton[0].click();

        // Making sure we have selected buttons effect.
        expect(await page.$x("//div[@class='diets']//button[@class='btn vegetarian selected']")).not.toBe(undefined);
        expect(await page.$x("//div[@class='diets']//button[@class='btn carnivore selected']").length).toBe(undefined);
        expect(await page.$x("//div[@class='diets']//button[@class='btn vegan selected']").length).toBe(undefined);

        buttons = await page.$x("//div[@class='products']//button");
        expect(buttons.length).toBe(74);

        // Filter by carnivore
        dietButton = await page.$x("//div[@class='diets']//button[@class='btn carnivore']");

        await dietButton[0].click();

        // Making sure we have selected buttons effect.
        expect(await page.$x("//div[@class='diets']//button[@class='btn carnivore selected']")).not.toBe(undefined);
        expect(await page.$x("//div[@class='diets']//button[@class='btn vegetarian selected']").length).toBe(undefined);
        expect(await page.$x("//div[@class='diets']//button[@class='btn vegan selected']").length).toBe(undefined);

        buttons = await page.$x("//div[@class='products']//button");
        expect(buttons.length).toBe(91);
    });

    it('Testing the items click', async () => {
        expect.assertions(2);

        let button = await page.$x("//div[@class='products']//button[contains(@class, 'selected')]");
        expect(button.length).toBe(0);

        button = await page.$x("//div[@class='products']//button");
        await button[0].click();

        button = await page.$x("//div[@class='products']//button[contains(@class, 'selected')]");
        expect(button.length).toBe(1);
    });

    afterAll(async () => {
        await browser.close();
    });
});
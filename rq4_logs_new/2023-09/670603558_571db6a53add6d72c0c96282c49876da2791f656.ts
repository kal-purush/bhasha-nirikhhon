import chalk from 'chalk';
import { PageHelper } from '../BaseHelper/page.helper';

export class MenucardHelper extends PageHelper {
    public async init() {
        await this.navigateTo('DASHBOARD');
    }

    public async openAndCloseMenuCard() {
        await this._page
            .locator(`//div[contains(@class,'hamburger_button')]`)
            .click();
    }

    public async getSideBarItems() {
        const items = await this._page.locator(
            `//div[contains(@class,'sidebar')]//p`
        );
        const itemTexts = await items.evaluate((item) => item.textContent);
        console.log(chalk.greenBright(itemTexts));
        return itemTexts;
    }
}
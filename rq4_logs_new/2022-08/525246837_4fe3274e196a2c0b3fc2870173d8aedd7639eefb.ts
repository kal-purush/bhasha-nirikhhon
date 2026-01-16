import { Page } from 'playwright';
import { EthereumConfig, WIDGET_URL } from './ethereum.constants';
import expect from 'expect';
import { Logger } from '@nestjs/common';
import { WalletPage } from '@lidofinance/wallets-testing-wallets';

export class EthereumPage {
  private readonly logger = new Logger(EthereumPage.name);
  page: Page;

  constructor(page: Page, private config: EthereumConfig) {
    this.page = page;
  }

  async navigate() {
    await this.page.goto(WIDGET_URL);
  }

  async connectWallet(walletPage: WalletPage) {
    await this.page.waitForTimeout(2000);
    const popup =
      (await this.page
        .locator("button :has-text('Close and proceed')")
        .count()) > 0;
    if (popup) await this.page.click("button :has-text('Close and proceed')");
    const isConnected =
      (await this.page
        .locator("button :has-text('Connect wallet')")
        .count()) === 0;
    if (!isConnected) {
      await this.page
        .locator("button :has-text('Connect wallet')")
        .first()
        .click();
      await this.page.waitForTimeout(2000);
      if ((await this.page.locator('text=Submit').count()) === 0) {
        if (!(await this.page.locator('input[type=checkbox]').isChecked()))
          await this.page.click('input[type=checkbox]', { force: true });
        if (walletPage.config.COMMON.SIMPLE_CONNECT) {
          await this.page
            .locator(
              `button[type=button] :text('${walletPage.config.COMMON.CONNECT_BUTTON_NAME}')`,
            )
            .click();
        } else {
          const [connectWalletPage] = await Promise.all([
            this.page.context().waitForEvent('page', { timeout: 5000 }),
            this.page
              .locator(
                `button[type=button] :text('${walletPage.config.COMMON.CONNECT_BUTTON_NAME}')`,
              )
              .click(),
          ]);
          await walletPage.connectWallet(connectWalletPage);
        }
        await this.page.waitForTimeout(1000);
        expect(await this.page.locator('text=Submit').count()).toBe(1);
      }
    }
  }

  async doStaking(walletPage: WalletPage) {
    await this.page
      .locator('input[type=text]')
      .fill(String(this.config.stakeAmount));
    const [walletSignPage] = await Promise.all([
      this.page.context().waitForEvent('page', { timeout: 90000 }),
      this.page.click('button[type=submit]'),
    ]);

    await walletPage.assertTxAmount(
      walletSignPage,
      String(this.config.stakeAmount),
    );
    await walletPage.confirmTx(walletSignPage);
  }
}
import { Inject, Injectable } from '@nestjs/common';
import * as ganache from 'ganache';
import { Server } from 'ganache';
import {
  Request,
  APIRequestContext,
  APIResponse,
  BrowserContext,
  Page,
} from 'playwright';
import { utils } from 'ethers';
import { OPTIONS, ServiceUnreachableError } from './node.constants';

@Injectable()
export class EthereumNodeService {
  readonly PORT = 7545;
  readonly nodeUrl = `http://127.0.0.1:${this.PORT}`;
  public static readonly defaultBalance = 6700;
  public static readonly stakeAmount = '100';

  state: { node: Server; account: string; secretKey: string } | undefined;

  constructor(@Inject(OPTIONS) private options: { rpcUrl: string }) {}

  async startNode() {
    if (this.state !== undefined) return;
    const node = ganache.server({
      chainId: 0x1,
      fork: { url: this.options.rpcUrl },
      logging: { quiet: true },
      wallet: { defaultBalance: EthereumNodeService.defaultBalance },
    });
    await node.listen(this.PORT);
    const initialAccounts = await node.provider.getInitialAccounts();
    const accounts = await node.provider.request({
      method: 'eth_accounts',
      params: [],
    });
    const account = accounts[0];
    const secretKey = initialAccounts[account].secretKey;
    await node.provider.send('evm_setAccountNonce', [
      account,
      '0x' + Math.floor(Math.random() * 9) + 1,
    ]);
    node.on('close', async () => {
      this.state = undefined;
    });
    this.state = { node, account, secretKey };
  }

  async stopNode() {
    if (this.state !== undefined) await this.state.node.close();
  }

  async getBalance() {
    if (this.state === undefined) return undefined;
    const response = await this.state.node.provider.request({
      method: 'eth_getBalance',
      params: [this.state.account, 'latest'],
    });
    return utils.formatEther(response);
  }

  async mockRoute(url: string, contextOrPage: BrowserContext | Page) {
    await contextOrPage.route(url, async (route) => {
      const response = await this.fetchSafety(
        contextOrPage.request,
        this.nodeUrl,
        {
          method: route.request().method(),
          data: route.request().postData(),
        },
      );
      return route.fulfill({
        response: response,
      });
    });
  }

  async fetchSafety(
    request: APIRequestContext,
    urlOrRequest: string | Request,
    options: any,
  ): Promise<APIResponse> {
    let lastErr;
    options.timeout = 0;
    options.headers = { Connection: 'Keep-Alive', 'Keep-Alive': 'timeout=1' };
    for (let tryCount = 0; tryCount < 3; tryCount++) {
      try {
        return await request.fetch(urlOrRequest, options);
      } catch (err) {
        lastErr = err as { message: string };
      }
    }
    // it causes if we force recreate browser context and there is no problem
    if (
      lastErr !== undefined &&
      !String(lastErr.message).includes(
        'Target page, context or browser has been closed',
      )
    )
      throw new ServiceUnreachableError(lastErr, options);
    throw Error("There's no response");
  }
}
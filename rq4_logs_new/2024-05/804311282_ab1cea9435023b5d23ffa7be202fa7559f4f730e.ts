import { OK } from 'http-status';
import { config } from '../config';
import retry from 'async-retry';
import { axiosInstance } from '../utils';
import { BlockNumberResponse, BlockRequest } from '../types';
import { UnprocessableEntityException } from '../utils';

const { rpcBaseUrls } = config.app;

class RPCPoolManager {
  readonly endpoints: string | string[];
  private currentIndex: number;
  private numEndpoints: number;

  constructor(endpoints: string | string[]) {
    const urls =
      typeof endpoints === 'string' ? JSON.parse(endpoints) : endpoints;
    this.endpoints = urls;
    this.numEndpoints = this.endpoints?.length || 0;
    this.currentIndex = 0;
  }

  async isRPCAvailable(endpoint: string): Promise<boolean> {
    try {
      const params: BlockRequest = {
        jsonrpc: '2.0',
        method: 'eth_blockNumber',
        params: [],
        id: 1,
      };

      const { status } = await axiosInstance.post<{
        status: number;
        data: BlockNumberResponse;
      }>(endpoint, params);

      return status === OK;
    } catch (error) {
      return false;
    }
  }

  async getCurrentEndpoint(): Promise<string> {
    try {
      for (let i = 0; i < this.numEndpoints; i++) {
        const endpoint = this.endpoints[this.currentIndex];

        this.currentIndex = (this.currentIndex + 1) % this.numEndpoints;

        if (await this.isRPCAvailable(endpoint)) {
          return endpoint;
        }
      }

      throw new UnprocessableEntityException('All RPC endpoints are down');
    } catch (error) {
      throw new UnprocessableEntityException('All RPC endpoints are down');
    }
  }

  async sendRequest<T>(
    method: string,
    params: [string, boolean] | [],
  ): Promise<T> {
    return retry(
      async () => {
        const endpoint = await this.getCurrentEndpoint();

        try {
          const { data } = await axiosInstance.post<T>(endpoint, {
            jsonrpc: '2.0',
            method,
            params,
            id: 1,
          });

          return data;
        } catch (error) {
          throw new UnprocessableEntityException(
            `request failed for endpoint: ${endpoint}`,
          );
        }
      },
      { retries: this.numEndpoints },
    );
  }
}

export const rpcPoolManager: RPCPoolManager = new RPCPoolManager(rpcBaseUrls);
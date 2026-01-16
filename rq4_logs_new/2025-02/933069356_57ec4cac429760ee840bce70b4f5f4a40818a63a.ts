import axios from 'axios';

export class PriceOracle {
  private static async fetchXionPrice(): Promise<number> {
    try {
      // Replace with actual oracle API endpoint
      const response = await axios.get('https://api.coingecko.com/api/v3/simple/price', {
        params: {
          ids: 'xion-finance',
          vs_currencies: 'usd'
        }
      });
      return response.data['xion-finance'].usd;
    } catch (error) {
      console.error('Failed to fetch XION price:', error);
      throw new Error('Failed to fetch current XION price');
    }
  }

  public static async getXionPriceInUSD(): Promise<number> {
    return await this.fetchXionPrice();
  }
}
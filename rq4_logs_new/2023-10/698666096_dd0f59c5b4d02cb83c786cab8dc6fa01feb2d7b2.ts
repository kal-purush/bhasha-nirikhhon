import { CryptoMarketApiClient, ExchangeRateNotFound, SingleRate } from "./CryptoMarketApiClient";
import { NbpApiClient } from "./NbpApiClient";
import { buildDateString } from "./buildDateString";

interface BinanceApiSingleTickerPrice {
    price: string;
    symbol: string; // BTCPLN
}
type BinanceApiTickerPriceResponse = Array<BinanceApiSingleTickerPrice>;
//https://api.binance.com/api/v3/ticker/price
export class BinanceApiClient implements CryptoMarketApiClient {
    private cache: BinanceApiTickerPriceResponse | null = null;
    private cachedKeys: Set<string> | null = null;
    private acceptedFiats: string[];
    private nbpClient: NbpApiClient;

    constructor(nbpClient: NbpApiClient, acceptedFiats: string[]) {
        this.acceptedFiats = acceptedFiats;
        this.nbpClient = nbpClient;
    }
    async fetchRate(cryptocurrencyCode: string): Promise<SingleRate> {
        if(!this.cache || !this.cachedKeys) {
            const tickerResponse = await fetch('https://api.binance.com/api/v3/ticker/price', {
                headers: {
                    accept: 'application/json'
                }
            });
            const parsedResponse = (await tickerResponse.json()) as BinanceApiTickerPriceResponse;
            this.cache = parsedResponse;
            this.cachedKeys = new Set(parsedResponse.map(entry => entry.symbol));
        }

        const keys = this.acceptedFiats.map(fiat => `${cryptocurrencyCode}${fiat}`);
        const key = keys.find(k => this.cachedKeys!.has(k));
        if(!key) {
            throw new ExchangeRateNotFound(cryptocurrencyCode, 'binance'); 
        }
        const rate = this.cache.find(e => e.symbol === key)!;
        const fiat = this.acceptedFiats[keys.indexOf(key)]!;
        const nbpRate = await this.nbpClient.fetchRate(fiat);
        return {
            exchangeRateEffectiveDateString: nbpRate.effectiveDate,
            exchangeRateToPLN: nbpRate.rate,
            unitPrice: parseFloat(rate.price),
            unitPriceCurrency: fiat
        }
    }

}
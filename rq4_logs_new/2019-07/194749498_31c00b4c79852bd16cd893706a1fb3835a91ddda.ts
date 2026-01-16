import { Quote, Overview } from '../../util';

interface Header {
  sector: string;
  exchange: string;
  latestPrice: number;
  change: number;
  changePercent: number;
  symbol: string;
  companyName: string;
}

const defaultSelector: Header = {
  sector: undefined,
  exchange: undefined,
  latestPrice: undefined,
  change: undefined,
  changePercent: undefined,
  companyName: undefined,
  symbol: undefined,
};

export function headerSelector(
  { overview }: { overview: Overview },
  { quote }: { quote: Quote }
) {
  return quote
    ? {
        sector: overview.sector,
        exchange: overview.exchange,
        latestPrice: quote.latestUpdate,
        change: quote.changePercent,
        changePercent: quote.change,
        companyName: quote.companyName,
        symbol: overview.symbol,
      }
    : defaultSelector;
}
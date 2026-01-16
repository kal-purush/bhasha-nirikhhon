import { Quote, KeyStats } from '../../util';

export function keyStatsSelector(stats: KeyStats, quote: Quote) {
  return stats && quote
    ? {
        companyStatsLeft: [
          {
            value: quote.close,
            name: 'Previous Close',
          },
          {
            name: 'Day Range',
            value: {
              high: quote.high,
              low: quote.low,
            },
          },
          {
            value: quote.latestVolume.toLocaleString(),
            name: 'Volume',
          },
          {
            value: quote.marketCap.toLocaleString(),
            name: 'Market Cap',
          },
          {
            value: quote.peRatio,
            name: 'P/E Ratio',
          },
        ],
        companyStatsRight: [
          {
            value: quote.open,
            name: 'Open',
          },
          {
            name: '52 Week Range',
            value: {
              high: quote.week52High,
              low: quote.week52Low,
            },
          },
          {
            value: quote.avgTotalVolume.toLocaleString(),
            name: 'Total Avg Volume',
          },
          {
            //TODO change to actualEPS when data is available
            value: stats.ttmEPS,
            name: 'Earnings Per Share',
          },
          {
            value: `${Number(stats.dividendYield * 100).toFixed(2)}%`,
            name: 'Dividend & Yield',
          },
        ],
      }
    : {};
}
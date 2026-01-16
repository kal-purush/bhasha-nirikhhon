using System;

namespace AM.Core.Algorithms.Puzzles
{
    public class BestStockTrades
    {
        public (int, int) GetBestTradeDays(int[] stockPrices)
        {
            int bestBuyDay = 0;
            int bestSellDay = 0;
            Tuple<int, int> bestDays = Tuple.Create(0, 0);
            int result = 0;
            for (int buyDay = 0; buyDay < stockPrices.Length; buyDay++)
            {
                for (int sellDay = buyDay + 1; sellDay < stockPrices.Length; sellDay++)
                {
                    var profit = stockPrices[sellDay] - stockPrices[buyDay];
                    if (profit > result)
                    {
                        bestBuyDay = buyDay;
                        bestSellDay = sellDay;
                        result = profit;
                    }
                }
            }

            return (bestBuyDay, bestSellDay);
        }
    }
}
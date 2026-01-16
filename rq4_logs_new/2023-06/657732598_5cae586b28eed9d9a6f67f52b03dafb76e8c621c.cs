using System;
using System.Collections.Generic;

namespace RealworldIntFINAL;

public class StockDictionary
{
    public Dictionary<string, decimal> Stocks { get; set; }

    public StockDictionary()
    {
        Stocks = new Dictionary<string, decimal>();
    }

    public void AddOrUpdateStock(string symbol, decimal price)
    {
        if (Stocks.ContainsKey(symbol))
        {
            Stocks[symbol] = price;
        }
        else
        {
            Stocks.Add(symbol, price);
        }
    }
}

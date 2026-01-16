using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
namespace RealworldIntFINAL;
public class StockSymbolProvider
{
    private readonly HttpClient _httpClient;

    public StockSymbolProvider()
    {
        _httpClient = new HttpClient();
    }

    public async Task<List<string>> GetUSStockSymbols(string apiKey)
    {
        string jsonResponse = await FetchStockSymbols(apiKey);
        StockSymbolsResponse response = JsonSerializer.Deserialize<StockSymbolsResponse>(jsonResponse);

        List<string> usStockSymbols = new List<string>();
        foreach (StockSymbol symbol in response.Data)
        {
            usStockSymbols.Add(symbol.Symbol);
        }

        return usStockSymbols;
    }

    private async Task<string> FetchStockSymbols(string apiKey)
    {
        string url = $"https://api.twelvedata.com/stocks?country=United%20States&apikey={apiKey}";
        HttpResponseMessage response = await _httpClient.GetAsync(url);

        if (response.IsSuccessStatusCode)
        {
            string jsonResponse = await response.Content.ReadAsStringAsync();
            return jsonResponse;
        }
        else
        {
            throw new Exception("Failed to retrieve stock symbols.");
        }
    }
}

/* Should be initilized once using the following commands in controller:
 * StockSymbolProvider symbolProvider = new StockSymbolProvider();
 * List<string> usStockSymbols = await symbolProvider.GetUSStockSymbols(apiKey);
 * Just creates a new instance of the class and then stores the string list with just US Stocks. This allows us later for user input to validate
 * it so that it is compliant with API rules that restricts it to US Markets.
 * Also with universal list of Symbols we can actually also use multiple APIs while making sure it is always compatible with our main one (TwelveData)
 * Ie with the list, when the user is adding a stock and therefore writing a symbol we can (after validating with the list) get its info with AlphaVantage that has much more generous
 * limits as well get closing stock price from AV if the market is closed. This is for if we have sufficient time to min/max with the resources we have
*/
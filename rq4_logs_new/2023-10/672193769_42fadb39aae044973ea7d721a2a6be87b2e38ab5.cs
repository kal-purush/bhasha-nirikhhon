using Flurl;
using Flurl.Http;
using Microsoft.Extensions.Options;

namespace api.CoinMarketCap.Service;
public class CoinMarketCapService : ICoinMarketCapService
{
    private readonly string endpoint = "v2/cryptocurrency/quotes/latest";
    private readonly CoinMarketCapSettings _coinMarketCapSettings;
    public CoinMarketCapService(IOptions<CoinMarketCapSettings> coinMarketCapSettings)
    {
        _coinMarketCapSettings = coinMarketCapSettings.Value;
    }
    public async Task<GetQuoteResponse> GetQuoteBySymbol(string symbol)
    {
        try
        {
            var response = await _coinMarketCapSettings.BaseUrl
                                .AppendPathSegment(endpoint)
                                .WithHeader(_coinMarketCapSettings.Header, _coinMarketCapSettings.ApiKey)
                                .SetQueryParam("symbol", symbol)
                                .GetJsonAsync<GetQuoteResponse>();

            return response;
        }
        catch (FlurlHttpException ex)
        {
            Console.WriteLine(ex.Message);
            throw;
        }

    }
}
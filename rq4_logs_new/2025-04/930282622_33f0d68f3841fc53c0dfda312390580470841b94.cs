using Bank.Application.Endpoints;
using Bank.Application.Responses;
using Bank.ExchangeService.Configurations;

namespace Bank.ExchangeService.Extensions;

public static class HttpClientExtensions
{
    public static async Task<CurrencyResponse?> GetCurrencyById(this HttpClient httpClient, Guid id)
    {
        var response = await httpClient.GetAsync($"{Configuration.Application.UserServiceUrl}/{Endpoints.Currency.Base}/{id}");

        if (!response.IsSuccessStatusCode)
            return null;

        return await response.Content.ReadFromJsonAsync<CurrencyResponse>();
    }

    public static async Task<CurrencySimpleResponse?> GetCurrencyByIdSimple(this HttpClient httpClient, Guid id)
    {
        var response = await httpClient.GetAsync($"{Configuration.Application.UserServiceUrl}/{Endpoints.Currency.GetAllSimple}/{id}");

        if (!response.IsSuccessStatusCode)
            return null;

        return await response.Content.ReadFromJsonAsync<CurrencySimpleResponse>();
    }

    public static async Task<List<CurrencySimpleResponse>?> GetAllCurrenciesSimple(this HttpClient httpClient)
    {
        var response = await httpClient.GetAsync($"{Configuration.Application.UserServiceUrl}/{Endpoints.Currency.GetAllSimple}");

        if (!response.IsSuccessStatusCode)
            return null;

        return await response.Content.ReadFromJsonAsync<List<CurrencySimpleResponse>>();
    }
}
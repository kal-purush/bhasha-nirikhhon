using System.Net;

using Handlers;

using Models;

namespace Services;

public static class AddressService
{
    public static async Task<Address> Get(string postalCode, Address fallbackAddress)
    {
        string baseUrl = "https://viacep.com.br/ws/";
        string endpoint = $"{UtilsHandler.OnlyNumbers(postalCode)}/json";

        Uri uri = new($"{baseUrl}/{endpoint}");

        (Address? response, HttpStatusCode statusCode) = await HttpService.GetRequest<Address>(uri, null, null);

        if (response is null || statusCode != HttpStatusCode.OK || response.Street is null || response.Street == String.Empty)
        {
            return fallbackAddress;
        }

        return response;
    }
}
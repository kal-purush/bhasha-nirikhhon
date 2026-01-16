namespace KlockanAPI.Application.Client;

public class CustomHttpClient : ICustomHttpClientService
{
    private readonly HttpClient _customHttpClient;

    public CustomHttpClient()
    {
        var httpClientHandler = new HttpClientHandler();
        httpClientHandler.ServerCertificateCustomValidationCallback = (message, cert, chain, errors) => true;

        _customHttpClient = new HttpClient(httpClientHandler);
    }

    public HttpClient GetCustomHttpClient()
    {
        return _customHttpClient;
    }
}
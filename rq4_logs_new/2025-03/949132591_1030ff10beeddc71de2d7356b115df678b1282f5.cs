using Microsoft.Extensions.Configuration;
using Soenneker.Domainr.Client.Abstract;
using Soenneker.Utils.HttpClientCache.Abstract;
using Soenneker.Utils.HttpClientCache.Dtos;
using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.Extensions.Configuration;

namespace Soenneker.Domainr.Client;

/// <inheritdoc cref="IDomainrClientUtil"/>
public class DomainrClientUtil : IDomainrClientUtil
{
    private readonly IHttpClientCache _httpClientCache;
    private readonly IConfiguration _configuration;

    public DomainrClientUtil(IHttpClientCache httpClientCache, IConfiguration configuration)
    {
        _httpClientCache = httpClientCache;
        _configuration = configuration;
    }

    public ValueTask<HttpClient> Get(CancellationToken cancellationToken = default)
    {
        var host = _configuration.GetValueStrict<string>("Domainr:Host");
        var apiKey = _configuration.GetValueStrict<string>("Domainr:ApiKey");

        var options = new HttpClientOptions
        {
            BaseAddress = $"https://{host}/v2/",
            DefaultRequestHeaders = new System.Collections.Generic.Dictionary<string, string>
            {
                {"x-rapidapi-key", apiKey},
                {"x-rapidapi-host", host}
            }
        };

        return _httpClientCache.Get(nameof(DomainrClientUtil), options, cancellationToken);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);

        _httpClientCache.RemoveSync(nameof(DomainrClientUtil));
    }

    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);

        return _httpClientCache.Remove(nameof(DomainrClientUtil));
    }
}
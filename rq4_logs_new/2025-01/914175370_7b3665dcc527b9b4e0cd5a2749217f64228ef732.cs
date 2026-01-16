using System.Net;
using NLog;
using PenumbraModForwarder.Common.Interfaces;

namespace PenumbraModForwarder.Common.Factory;

public class XmaHttpClientFactory : IXmaHttpClientFactory
{
    private static readonly Logger _logger = LogManager.GetCurrentClassLogger();

    private readonly IConfigurationService _configurationService;

    // Keep track of the most recent cookie so we can detect changes.
    private string? _lastCookieValue;

    // Reuse a single HttpClient if the cookie hasn't changed to avoid repeated creation.
    private HttpClient? _cachedClient;

    public XmaHttpClientFactory(IConfigurationService configurationService)
    {
        _configurationService = configurationService;
    }

    /// <inheritdoc />
    public HttpClient CreateClient()
    {
        var cookieValue = (string)_configurationService.ReturnConfigValue(c => c.AdvancedOptions.XIVModArchiveCookie);
        
        // If there's no cookie, return a default HttpClient
        if (string.IsNullOrWhiteSpace(cookieValue))
        {
            // Create a new client only if we don't already have one cached or if we previously had a cookie-based client
            if (_cachedClient == null || _lastCookieValue != null)
            {
                _logger.Debug("Returning a new standard HttpClient with no cookie.");
                _cachedClient = new HttpClient();
                _lastCookieValue = null;
            }

            return _cachedClient;
        }
        
        // If the cookie hasn't changed, return the cached HttpClient
        if (cookieValue == _lastCookieValue && _cachedClient != null)
        {
            _logger.Debug("Reusing HttpClient with unchanged cookie.");
            return _cachedClient;
        }
        
        
        // Otherwise, build a new HttpClient with the updated cookie
        _logger.Debug("Creating a new HttpClient due to updated cookie.");
        var handler = new HttpClientHandler
        {
            CookieContainer = new CookieContainer()
        };

        // Use the same domain required by XIV Mod Archive
        var cookieUri = new Uri("https://www.xivmodarchive.com");
        handler.CookieContainer.Add(cookieUri, new Cookie("connect.sid", cookieValue));

        _cachedClient = new HttpClient(handler);
        _lastCookieValue = cookieValue;

        return _cachedClient;
    }
}
using Microsoft.AspNetCore.Mvc;
using Rinkudesu.Gateways.Clients;
using Rinkudesu.Gateways.Webui.Utils;

namespace Rinkudesu.Gateways.Webui.Controllers;

public abstract class AccessTokenClientControllerBase<TClient> : ControllerBase where TClient : AccessTokenClient
{
    private readonly TClient _client;
    private bool tokenSet;

    protected TClient Client
    {
        get
        {
            if (tokenSet)
                return _client;

            _client.SetAccessToken(HttpContext.GetJwt());
            tokenSet = true;
            return _client;
        }
    }

    protected AccessTokenClientControllerBase(TClient client)
    {
        _client = client;
    }
}
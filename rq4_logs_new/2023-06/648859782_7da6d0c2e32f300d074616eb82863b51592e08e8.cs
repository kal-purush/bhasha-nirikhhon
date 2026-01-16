namespace IDPLibrary.MemoryClientProvider;
internal class ClientProvider : IClientProvider
{
    readonly IReadOnlyList<Client> Clients;
    public ClientProvider(IOptions<MemoryClientListOptions> options)
    {
        Clients = new List<Client>(options.Value.Clients);
    }
    public Task<Client> GetClientAsync(string clientId, string redirectUri) =>

        clientId == "client1" && redirectUri == "https://localhost/callback" ?
        Task.FromResult(new Client { ClientId = clientId, RedirectUris = new string[] { redirectUri } }) :
        Task.FromResult<Client>(null);
}
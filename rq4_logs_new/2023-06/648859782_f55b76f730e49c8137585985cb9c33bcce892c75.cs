namespace IDPLibrary.Clients;
internal class ClientProvider : IClientProvider
{
    readonly IReadOnlyList<Client> Clients;

    public ClientProvider(IOptions<ClientList> options)
    {
        Clients = new List<Client>(options.Value.Clients);
    }

    public Task<Client> GetClientAsync(string clientId, string redirectUri)
    {
        Client client = null;
        ClientSpecification specification = new(clientId, redirectUri);
        if(specification.IsValidated())
            client = Clients.Where(c => c.ClientId == clientId && c.RedirectUris.Contains(redirectUri)).FirstOrDefault();
        return Task.FromResult(client);
    }
}
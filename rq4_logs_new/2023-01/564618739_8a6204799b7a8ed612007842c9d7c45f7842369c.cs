namespace Mastodon.Client;

public sealed class BookmarkClient
{
    private readonly MastodonClient _client;

    internal BookmarkClient(MastodonClient client)
    {
        _client = client;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="offset">Skip the first n results.</param>
    /// <param name="limit">How many accounts to load. Defaults to 40 accounts. Max 80 accounts.</param>
    /// <param name="order">Use active to sort by most recently posted statuses (default) or new to sort by most recently created profiles.</param>
    /// <param name="local">If true, returns only local accounts.</param>
    public Task<Response<List<Status>>> GetBookmarksAsync(string? maxId = null, string? sinceId = null, string? minId = null, uint? limit = null)
    {
        var q = new QueryBuilder();

        q.Add("max_id", maxId);
        q.Add("since_id", sinceId);
        q.Add("min_id", minId);
        q.Add("limit", limit);

        var url = q.GetUrl("api/v1/bookmarks");

        return _client.HttpClient.GetFromJsonWithHeadersAsync<List<Status>>(url, MastodonClient._options);
    }
}
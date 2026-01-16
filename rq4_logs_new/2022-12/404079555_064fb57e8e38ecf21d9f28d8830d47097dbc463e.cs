using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Rinkudesu.Gateways.Utils;

namespace Rinkudesu.Gateways.Clients.Tags;

public class TagsClient : AccessTokenClient
{
    public TagsClient(HttpClient client, ILogger<TagsClient> logger) : base(client, logger)
    {
    }

    public async Task<TagDto?> GetTag(Guid id, CancellationToken cancellationToken = default)
    {
        try
        {
            using var message = await Client.GetAsync($"tags/{id.ToString()}".ToUri(), cancellationToken).ConfigureAwait(false);
            return await HandleMessageAndParseDto<TagDto>(message, id.ToString(), cancellationToken).ConfigureAwait(false);
        }
        catch (HttpRequestException e)
        {
            Logger.LogWarning(e, "Error while requesting tag with id '{Id}'", id.ToString());
            return null;
        }
    }

    public async Task<bool> CreateTag(TagDto newTag, CancellationToken cancellationToken = default)
    {
        try
        {
            using var content = GetJsonContent(newTag);
            var response = await Client.PostAsync("tags".ToUri(), content, cancellationToken).ConfigureAwait(false);
            if (response.IsSuccessStatusCode) return true;
            Logger.LogWarning("Unable to create new tag. Response code was '{StatusCode}'.", response.StatusCode);
            return false;
        }
        catch (JsonException e)
        {
            Logger.LogWarning(e, "Unable to serialise new tag into json");
            return false;
        }
        catch (HttpRequestException e)
        {
            Logger.LogWarning(e, "Error while requesting new tag creation.");
            return false;
        }
    }

    public async Task<IEnumerable<TagDto>?> GetTags(CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await Client.GetAsync("tags".ToUri(), cancellationToken).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
            {
                Logger.LogWarning("Received non-success status code '{StatusCode}' from tags microservice", response.StatusCode);
                return null;
            }
            return await HandleMessageAndParseDto<IEnumerable<TagDto>>(response, "all tags", cancellationToken).ConfigureAwait(false);
        }
        catch (JsonException e)
        {
            Logger.LogWarning(e, "Unable to parse tags");
            return null;
        }
        catch (HttpRequestException e)
        {
            Logger.LogWarning(e, "Unable to receive tags from microservice");
            return null;
        }
    }

    public async Task<bool> Delete(Guid id, CancellationToken token = default)
    {
        try
        {
            var response = await Client.DeleteAsync($"tags/{id}".ToUri(), token).ConfigureAwait(false);
            return response.IsSuccessStatusCode;
        }
        catch (HttpRequestException e)
        {
            Logger.LogWarning(e, "Unable to delete tag with id '{id}'", id);
            return false;
        }
    }

    public async Task<bool> Edit(Guid id, TagDto tag, CancellationToken token = default)
    {
        try
        {
            using var content = GetJsonContent(tag);
            var response = await Client.PutAsync("tags".ToUri(), content, token).ConfigureAwait(false);
            if (response.IsSuccessStatusCode) return true;

            Logger.LogWarning("Unable to edit tag with id {Id}. Response code was '{StatusCode}'.", id.ToString(), response.StatusCode);
            return false;
        }
        catch (JsonException e)
        {
            Logger.LogWarning(e, "Unable to serialise tag into json");
            return false;
        }
        catch (HttpRequestException e)
        {
            Logger.LogWarning(e, "Error while requesting new tag creation.");
            return false;
        }
    }
}
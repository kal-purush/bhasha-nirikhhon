using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.NetworkInformation;
using System.Threading;
using System.Threading.Tasks;
using Ardalis.GuardClauses;
using AutoMapper;
using GitHubApiClient.Abstractions;
using GitHubApiClient.Models;
using GitHubCommunicationService.Abstractions;
using GitHubCommunicationService.Config;
using GitHubCommunicationService.Data.Models;
using GitHubCommunicationService.Responses;
using Microsoft.Extensions.Options;
using MongoDatabaseAdapter.Abstractions;
using MongoDatabaseAdapter.Settings;
using Newtonsoft.Json;
using RestSharp;
using RestSharp.Authenticators;

namespace GitHubCommunicationService.Services
{
    public class GitHubApiService : IGitHubApiService
    {
        private readonly IGitHubApiClient _gitHubApiClient;
        private readonly IMapper _mapper;

        public GitHubApiService(
            IGitHubApiClient gitHubApiClient,
            IMapper mapper)
        {
            _gitHubApiClient = Guard.Against.Null(gitHubApiClient, nameof(gitHubApiClient));
            _mapper = Guard.Against.Null(mapper, nameof(mapper));
        }

        public async Task<IEnumerable<GitHubUserRepositoryResponse>> GetUserRepositoriesFromApiAsync(CancellationToken ct = default)
        {
            var result = await _gitHubApiClient.GetRepositoriesForUserAsync(ct);
            
            if (!result.IsSuccessful)
                throw new Exception($"An error occured: {result.Message}");

            result = Guard.Against.Null(result, nameof(result));
            var json = result.Json;

            if (string.IsNullOrWhiteSpace(json))
                return Enumerable.Empty<GitHubUserRepositoryResponse>();
            
            return JsonConvert.DeserializeObject<IEnumerable<GitHubUserRepositoryResponse>>(json)!;
        }
    }
}
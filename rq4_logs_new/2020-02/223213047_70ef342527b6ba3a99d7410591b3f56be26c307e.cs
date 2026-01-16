using System;
using System.Collections.Generic;
using System.Text;
using DFC.App.MatchSkills.Application.Cosmos.Models;
using DFC.App.MatchSkills.Application.Cosmos.Services;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Options;
using Moq;
using NSubstitute;
using NUnit.Framework;

namespace DFC.App.MatchSkills.Application.Test.Unit.Services
{
    public class CosmosServiceTests
    {
        public class CreateDocumentAsyncTests
        {
            private IOptions<CosmosSettings> _cosmosSettings;
            //private CosmosClient _client;
            private CosmosService _service;
            private string _currentPage, _previousPage;

            [OneTimeSetUp]
            public void Init()
            {
                _cosmosSettings = Options.Create(new CosmosSettings()
                {
                    ApiUrl = "https://test-account-not-real.documents.azure.com:443/",
                    ApiKey = "VGhpcyBpcyBteSB0ZXN0",
                    DatabaseName = "DatabaseName",
                    UserSessionsCollection = "UserSessions"
                });
                //_client = new CosmosClient(accountEndpoint: _cosmosSettings.Value.ApiUrl,
                //    authKeyOrResourceToken: _cosmosSettings.Value.ApiKey);
            }

            [Test]
            public void WhenItemIsNull_ThrowException()
            {
                var client = Substitute.For<CosmosClient>();
                //Mock GetContainer call
                //client.When(x => x.GetContainer(default, default)).Do(null);
                _service = new CosmosService(_cosmosSettings, client);
            }
        }
    }
}
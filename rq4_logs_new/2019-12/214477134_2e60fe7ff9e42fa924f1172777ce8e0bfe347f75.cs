using CentralDeErros.Api.Test.Controllers.Base;
using CentralDeErros.Application.ViewModel;
using GraphQL.Client;
using GraphQL.Common.Request;
using System.Collections.Generic;
using System.Net.Http;
using Xunit;

namespace CentralDeErros.Api.GraphQL
{
    public class CentralDeErrosQueryTest : BaseControllerTest, IClassFixture<CustomWebApplicationFactory<Startup>>
    {
        private readonly HttpClient _client;
        public CentralDeErrosQueryTest(CustomWebApplicationFactory<Startup> factory) : base(factory)
        {
            _client = factory.CreateClient();
        }

        [Fact]
        public async void Should_get_all_users()
        {
            var erroLogRequest = new GraphQLRequest
            {
                Query = @"
	                query{
                      errorlogs {
                        id
                        archieved
                        code
                        environment
                        level
                        message
                      }
                    }"
            };

            var graphQLClient = new GraphQLClient("https://centraldeerroscsharp.herokuapp.com/graphql");
            var graphQLResponse = await graphQLClient.PostAsync(erroLogRequest);
            List<ErrorLogViewModel> users = graphQLResponse.GetDataFieldAs<List<ErrorLogViewModel>>("errorlogs");

        }
    }
}
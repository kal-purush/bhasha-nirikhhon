using DFC.App.MatchSkills.Controllers;
using DFC.App.MatchSkills.Interfaces;
using FluentAssertions;
using Microsoft.AspNetCore.Mvc;
using NSubstitute;
using NUnit.Framework;

namespace DFC.App.MatchSkills.Test.Unit.Controllers
{
    class ApiDefinitionControllerTests
    {
        [Test]
        public void When_ApiDefiniton_Then_ReturnApiDefinition()
        {
            var _fileService = Substitute.For<IFileService>();
            
            _fileService.ReadAllText("").ReturnsForAnyArgs("{\"openapi\":\"3.0.1\",\"info\":{\"title\":\"Test Match Skills, Search Occupation\",\"description\":\"Get occupation matches based on search term\",\"version\":\"1.0\"},\"servers\":[{\"url\":\"https:\\/\\/dev.api.nationalcareersservice.org.uk\\/matchskills\\/occupationsearchauto\"}],\"paths\":{\"\\/Execute\\/\":{\"get\":{\"tags\":[\"occupation\"],\"summary\":\"Occupation search\",\"description\":\"Get occupation matches based on search term\",\"operationId\":\"OccupationSearchAuto\",\"requestBody\":{\"description\":\"Supply occupation search term\",\"content\":{\"application\\/json\":{\"schema\":{\"$ref\":\"#\\/components\\/schemas\\/RequestBody\"},\"example\":{\"occupation\":\"renewable\"}}}},\"responses\":{\"200\":{\"description\":\"successful operation\",\"content\":{\"application\\/json; charset=utf-8\":{\"schema\":{\"$ref\":\"#\\/components\\/schemas\\/Occupation\"}}}},\"204\":{\"description\":\"No content can be found.\"}}}}},\"components\":{\"schemas\":{\"RequestBody\":{\"required\":[\"occupation\"],\"type\":\"object\",\"properties\":{\"occupation\":{\"type\":\"string\",\"example\":\"renewable\"}}},\"Occupation\":{\"type\":\"array\",\"example\":[\"Renewable energy consultant\",\"Energy trader\",\"Renewable energy sales representative\",\"Renewable energy engineer\"]}},\"securitySchemes\":{\"apiKeyHeader\":{\"type\":\"apiKey\",\"name\":\"Ocp-Apim-Subscription-Key\",\"in\":\"header\"},\"apiKeyQuery\":{\"type\":\"apiKey\",\"name\":\"subscription-key\",\"in\":\"query\"}}},\"security\":[{\"apiKeyHeader\":[]},{\"apiKeyQuery\":[]}],\"tags\":[{\"name\":\"Match Skills, Search Occupation\",\"description\":\"Get occupation matches based on search term\"}]}");
            var sut = new ApiDefinitionController(_fileService);
            var results = sut.Index();

            results.Should().NotBe(null);
            results.Should().BeOfType(typeof(OkObjectResult));
        }

        [Test] public void When_ApiDefinitonBlank_Then_ReturnNoContent()
        {
            var _fileService = Substitute.For<IFileService>();
            _fileService.ReadAllText("").ReturnsForAnyArgs("");
            var sut = new ApiDefinitionController(_fileService);
            var results = sut.Index();
            results.Should().NotBe(null);
            results.Should().BeOfType(typeof(NoContentResult));
        }
    }
}
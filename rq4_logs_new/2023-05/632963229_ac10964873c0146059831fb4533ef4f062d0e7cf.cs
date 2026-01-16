using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;
using Moq;
using MovieFiles.Api.Functions;
using MovieFiles.Core.Interfaces;
using MovieFiles.Core.Models.People;

namespace MovieFiles.Test
{
    public class PeopleFunctionTest
    {
        private readonly Mock<ILogger<People>> _loggerMock;
        private readonly Mock<IPeopleService> _peopleServiceMock;
        private readonly People _peopleFunction;

        public PeopleFunctionTest()
        {
            _loggerMock = new Mock<ILogger<People>>();
            _peopleServiceMock = new Mock<IPeopleService>();
            _peopleFunction = new People(_loggerMock.Object, _peopleServiceMock.Object);
        }

        [Fact]
        public async Task SearchPeople_ReturnsOkObjectResult()
        {
            // Arrange
            var mockHttpRequest = CreateMockHttpRequest("query", "test");
            int numberOfPersons = 10;  // You can specify any number here

            // Setup for SearchPeople method
            _peopleServiceMock
                .Setup(service => service.SearchPeople(It.IsAny<string>()))
                .ReturnsAsync(GeneratePeopleList(numberOfPersons));

            // Act
            var response = await _peopleFunction.SearchPeople(mockHttpRequest);

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(response);
            var returnValue = Assert.IsType<PeopleList>(okResult.Value);

            Assert.Equal(GeneratePeopleList(numberOfPersons).Results.Count, returnValue.Results.Count);
        }


        private HttpRequest CreateMockHttpRequest(string queryKey, string queryValue)
        {
            var context = new DefaultHttpContext();
            var request = context.Request;
            request.Query = new QueryCollection(new Dictionary<string, StringValues> { { queryKey, queryValue } });

            return request;
        }

        private PeopleList GeneratePeopleList(int numberOfPersons)
        {
            var mockPersonList = new List<Person>();

            for (int i = 1; i <= numberOfPersons; i++)
            {
                mockPersonList.Add(new Person() { Id = i, Name = $"Person{i}" });
            }

            return new PeopleList()
            {
                Page = 1,
                Results = mockPersonList,
                TotalPages = 1,
                TotalResults = mockPersonList.Count
            };
        }

    }
}
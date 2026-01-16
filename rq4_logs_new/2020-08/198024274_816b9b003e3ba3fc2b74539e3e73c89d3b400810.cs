using Moq;
using Nt.Domain.Entities.Exceptions;
using Nt.Domain.Entities.Movie;
using Nt.Domain.ServiceContracts.Movie;
using Nt.Infrastructure.WebApi.Controllers;
using Nt.Infrastructure.WebApi.ViewModels.Areas.Movie.CreateMovie;
using Nt.Infrastructure.WebApi.ViewModels.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Nt.Infrastructure.Tests.Controllers.MovieControllersTests
{
    public class CreateMovieTests : ControllerTestBase<MovieEntity>
    {
        public CreateMovieTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [MemberData(nameof(CreateMovieTestFailureCasesTestData))]
        public async Task CreateMovieTestFailureCases(CreateMovieRequest request,CreateMovieResponse expectedResult)
        {
            // Arrange
            var mockMovieService = new Mock<IMovieService>();
            mockMovieService.Setup(x => x.CreateAsync(It.IsAny<MovieEntity>())).Throws<EntityAlreadyExistException>();

            // Act
            var movieController = new MovieController(Mapper, mockMovieService.Object);
            var result = await movieController.CreateMovie(request);


            // Assert
            if(result is not IErrorInfo error)
            {
                throw new XunitException();
            }

            Assert.Equal(error.ErrorMessage, result.ErrorMessage);
            Assert.True(error.HasError);
        }


        public static IEnumerable<object[]> CreateMovieTestFailureCasesTestData => new[]
        {
            new object[] 
            { 
                new CreateMovieRequest { Title = "Title 1", Language="Malayalam", ReleaseDate =DateTime.Now  }, 
                new CreateMovieResponse { Title = "Title 1", Language = "Malayalam", ReleaseDate = DateTime.Now }  
            }
        };
    }
}
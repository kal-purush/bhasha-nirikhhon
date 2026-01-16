using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Web.Helpers;
using System.Web.Http.Results;
using Cinema.Controllers;
using Cinema.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;

namespace Cinema.Tests.Controllers
{
    [TestClass]
    public class MovieTests
    {
        // STUBBING vs MOCKING
        [TestMethod]
        [TestCategory("Positive tests")]
        public async Task CanCreateMovie()
        {
            //Arrange
            var controller = new MoviesController();
            var movieName = "Lord of the Rings";
            var movie = new Movie()
            {
                Name = movieName,
                Rating = 11
            };
            movie.Tickets.Add(new Ticket()
            {
                Price = 0.99
            });
            movie.Tickets.Add(new Ticket()
            {
                Price = 1.99
            });

            var actionResult = await controller.PostMovie(movie)
                               as CreatedAtRouteNegotiatedContentResult<Movie>;

            Assert.AreEqual(movieName, actionResult.Content.Name);
        }

        [TestMethod]
        [TestCategory("Positive tests")]
        public async Task CanDeleteMovie()
        {
            //Arrange
            var controller = new MoviesController();
            var movie = new Movie()
            {
                Name = "Lord of the Rings",
                Rating = 11
            };
            movie.Tickets.Add(new Ticket()
            {
                Price = 0.99
            });
            movie.Tickets.Add(new Ticket()
            {
                Price = 1.99
            });
            await controller.PostMovie(movie);

            var actionResult = await controller.DeleteMovie(movie.Id);
            
            Assert.IsInstanceOfType(actionResult, typeof(OkNegotiatedContentResult<Movie>));
        }

        [TestMethod]
        [TestCategory("Negative tests")]
        public async Task CanNotDeleteNotExistingMovie()
        {
            var controller = new MoviesController();
            var movieId = 42;

            var actionResult = await controller.DeleteMovie(movieId);

            Assert.IsInstanceOfType(actionResult, typeof(NotFoundResult));
        }
    }
}
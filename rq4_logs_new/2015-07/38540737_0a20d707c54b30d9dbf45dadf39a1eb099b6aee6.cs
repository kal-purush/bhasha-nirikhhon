using System;
using System.Threading.Tasks;
using System.Web.Http.Results;
using Cinema.Controllers;
using Cinema.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cinema.Tests.Controllers
{
    [TestClass]
    public class ShopTests
    {
        [TestMethod]
        public async Task BuyTicket()
        {
            var shopController = new ShopController();
            var movieController = new MoviesController();
            var ticketQuantity = 23;
            var movie = new Movie()
            {
                Name = "Batman",
                Rating = 6.7,
                Ticket = new Ticket()
                {
                    Price = 2.99,
                    Quantity = ticketQuantity
                }
            };
            await movieController.PostMovie(movie);

            var result = await shopController.BuyTicket(movie.Name) as OkNegotiatedContentResult<Ticket>;

            Assert.IsNotNull(result);
            Assert.AreEqual(ticketQuantity - 1, result.Content.Quantity);

            await movieController.DeleteMovie(movie.Name);
        }
    }
}
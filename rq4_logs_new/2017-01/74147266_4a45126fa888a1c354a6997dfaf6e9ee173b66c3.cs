using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using api.Controllers;
using api.Models.OutputModels;
using api.Models.Output;
using System.Collections.Generic;
using System.Data.Entity;
using Moq;
using api.DAL;
using System.Linq;
using api.Adapters;
using api.Models.Data;

namespace api.Tests.Controllers
{
    [TestClass]
    public class MoviesControllerTest
    {
       






        private Mock<MovieBotContext> context;
        [TestInitialize]
        public void initialize()
        {
            
        }

        [TestMethod]
        public void getMovieTest()
        {
            MoviesController controller = new MoviesController();
            var result = controller.GetMovieByTitle("itanic") as System.Web.Http.Results.OkNegotiatedContentResult<JsonApiOutput<IEnumerable<MovieOutputModel>>>;
            IEnumerable<MovieOutputModel> moviesList = result.Content.Data;
            foreach (var item in moviesList)
            {
                if (item.Title.Contains("itanic"))
                {
                    Assert.IsTrue(true);
                    return;
                }
            }
            Assert.Fail();
        }
    }
}
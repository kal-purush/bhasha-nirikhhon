using System;
using System.Net;
using System.Web.Http.Results;
using BroadwayBuilder.Api.Controllers;
using DataAccessLayer;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ServiceLayer.Services;

namespace BroadwayBuilder.Api.Tests.UnitTest
{
    [TestClass]
    public class UnitTestHelpWantedController
    {
        [TestMethod]
        public void CreateTheaterJob_Should_ReturnErrorMessage()
        {
            //Arrange
            var controller = new HelpWantedController();
            TheaterJobPosting job = null;

            //Act
            var actionResult = controller.CreateTheaterJob(job);
            var response = actionResult as NegotiatedContentResult<string>;
            var content = response.Content;

            //Assert
            Assert.IsNotNull(response);
            Assert.IsNotNull(response.Content);
            Assert.AreEqual((HttpStatusCode)400, response.StatusCode);
        }

        [TestMethod]
        public void EditTheaterJob_Should_ReturnErrorMessage()
        {
            //Arrange
            var controller = new HelpWantedController();
            TheaterJobPosting job = null;

            //Act
            var actionResult = controller.EditTheaterJob(job);
            var response = actionResult as NegotiatedContentResult<string>;
            var content = response.Content;

            //Assert
            Assert.IsNotNull(response);
            Assert.IsNotNull(response.Content);
            Assert.AreEqual((HttpStatusCode)400, response.StatusCode);
        }

        
    }
}
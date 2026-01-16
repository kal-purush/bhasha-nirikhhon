using System;
using System.Text;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CreativeCravings.Controllers;
using System.Web.Mvc;

namespace CreativeCravings.Tests.Controllers
{

    [TestClass]
    public class RecipesControllerTest
    {
        [TestMethod]
        public void Index()
        {
            // Arrange
            RecipesController controller = new RecipesController();

            // Act
            ViewResult result = controller.Index("", "", "", 1) as ViewResult;

            // Assert
            Assert.IsNotNull(result);
        }

    }
}
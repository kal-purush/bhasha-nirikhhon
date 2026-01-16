using Microsoft.VisualStudio.TestTools.UnitTesting;
using CreativeCravings.Controllers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Mvc;

namespace CreativeCravings.Controllers.Tests
{
    [TestClass]
    public class IngredientsControllerTests
    {
        [TestMethod]
        public void Create()
        {
            // Arrange
            IngredientsController controller = new IngredientsController();

            // Act
            ViewResult result = controller.Create() as ViewResult;

            // Assert
            Assert.IsNotNull(result);
        }
    }
}
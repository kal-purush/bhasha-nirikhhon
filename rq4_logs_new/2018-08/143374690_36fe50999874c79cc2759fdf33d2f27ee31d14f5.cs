using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Web.Http;

using WebApplicationTaskB;
using WebApplicationTaskB.Controllers;


namespace WebApplicationTaskB.Tests.Controllers
{
    [TestClass]
    public class ExampleControllerTests
    {
        [TestMethod]
        public void TestMethod1()
        {
            // Arrange
            ExampleController controller = new ExampleController();

            // Act
            IEnumerable<string> result = controller.Get();

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2, result.Count());
            Assert.AreEqual("Wrong", result.ElementAt(0));
            Assert.AreEqual("value2", result.ElementAt(1));
        }
    }
}
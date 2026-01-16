using Microsoft.AspNetCore.Mvc;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WebStore.Controllers;
using WebStore.Interfaces.TestApi;
using Assert = Xunit.Assert;

namespace WebStore.UI.Tests.Controllers
{
    [TestClass]
    public class WebApiControllerTests
    {
        [TestMethod]
        public void Index_Returns_View_with_Values() {
            //Arrange
            Mock<IValuesService> valueServiceMock = new Mock<IValuesService>();
            IEnumerable<string> expectedStringValues = new[] { "1", "2", "3" };
            valueServiceMock.Setup((x) => x.Get()).Returns(() => expectedStringValues);

            WebAPIController controller = new WebAPIController(valueServiceMock.Object);

            //Act
            var actionResult =  controller.Index();

            //Assert
            ViewResult viewResult = Assert.IsType<ViewResult>(actionResult);
            Assert.Equal(expectedStringValues, (viewResult.Model as IEnumerable<string>));
            valueServiceMock.Verify(service => service.Get(), Times.Exactly(1));
            valueServiceMock.VerifyNoOtherCalls();
        }
    }
}
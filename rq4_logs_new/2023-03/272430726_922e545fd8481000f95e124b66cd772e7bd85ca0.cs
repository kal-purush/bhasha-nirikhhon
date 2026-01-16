using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using NUnit.Framework;
using SFA.DAS.Tools.Support.UnitTests.AutoFixture;
using SFA.DAS.Tools.Support.Web.Controllers;
using System.Security.Claims;

namespace SFA.DAS.Tools.Support.UnitTests
{
    public class HomeControllerTests
    {
        [Test, DomainAutoData]
        public void When_User_IsAuthenticated_Route_TheUser_To_Support(string username, string authType)
        {
            //arrange
            var controller = new HomeController();
            controller.ControllerContext = new ControllerContext
            {
                HttpContext = new DefaultHttpContext
                {
                    User = new ClaimsPrincipal(new ClaimsIdentity(new Claim[]
                    {
                        new Claim(ClaimTypes.Name, username)
                    }, authType))
                }
            };

            //sut
            var result = (RedirectToActionResult)controller.Index();

            //assert
            Assert.AreEqual("Index", result.ActionName);
            Assert.AreEqual("Support", result.ControllerName);
        }
    }
}
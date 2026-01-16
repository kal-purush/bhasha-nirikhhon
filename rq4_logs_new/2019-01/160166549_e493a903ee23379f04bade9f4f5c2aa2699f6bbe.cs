using Common.Constant;
using Microsoft.AspNetCore.Http;
using Moq;
using NUnit.Framework;
using ServiceTier.Services;
using System.Security.Claims;

namespace ServiceTierTests
{
    [TestFixture]
    public class UserIdentityServiceTests
    {
        private Mock<IHttpContextAccessor> httpContextAccessor;

        private UserIdentityService subject;

        [SetUp]
        public void Setup()
        {
            httpContextAccessor = new Mock<IHttpContextAccessor>();
            subject = new UserIdentityService(httpContextAccessor.Object);
        }

        [Test]
        public void GetUserId()
        {
            int userId = 1;
            var claim = new ClaimsIdentity();
            claim.AddClaim(new Claim(Constants.ClaimUserId, userId.ToString()));
            var httpContext = new Mock<HttpContext>();
            var user = new Mock<ClaimsPrincipal>();

            httpContextAccessor.Setup(_ => _.HttpContext).Returns(httpContext.Object);
            httpContext.Setup(_ => _.User).Returns(user.Object);
            user.Setup(_ => _.Identity).Returns(claim);

            var actual = subject.GetUserId();

            Assert.AreEqual(actual, userId);
        }
    }
}
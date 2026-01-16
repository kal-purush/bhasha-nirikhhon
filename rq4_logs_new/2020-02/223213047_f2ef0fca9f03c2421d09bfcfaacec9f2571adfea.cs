using System;
using System.Collections.Specialized;
using DFC.App.MatchSkills.Service;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Http;
using NSubstitute;
using NUnit.Framework;

namespace DFC.App.MatchSkills.Test.Service
{
    public class CookieServiceTests
    {
        [Test]
        public void When_AppendCookieCalled_Then_CookieIsUpdated()
        {
            var service = new CookieService(new EphemeralDataProtectionProvider());

            var response = Substitute.For<HttpResponse>();
            var sessionId = "Session";
            service.AppendCookie(sessionId, response);

            

            var protector = (new EphemeralDataProtectionProvider()).CreateProtector(nameof(CookieService));

            var session = protector.Protect(sessionId);

            response.Received().Cookies.Append(CookieService.CookieName, session, Arg.Any<CookieOptions>());
        }

        [Test]
        public void When_CookieFailsToDecrypt_ThenRemoveCookie()
        {
            var service = new CookieService(new EphemeralDataProtectionProvider());

            var response = Substitute.For<HttpResponse>();
            var request = Substitute.For<HttpRequest>();
            var requestCookie = Substitute.For<IRequestCookieCollection>();
            string data = "test";
            requestCookie.TryGetValue(Arg.Any<string>(), out data).ReturnsForAnyArgs(true);
            request.Cookies.ReturnsForAnyArgs(requestCookie);
            service.TryGetPrimaryKey(request, response);

            response.Received().Cookies.Delete(CookieService.CookieName);
        }
    }
}
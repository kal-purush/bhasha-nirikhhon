using DFC.App.MatchSkills.Application.Session.Interfaces;
using DFC.App.MatchSkills.Application.Session.Models;
using DFC.App.MatchSkills.Controllers;
using DFC.App.MatchSkills.Interfaces;
using FluentAssertions;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Abstractions;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using NSubstitute;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RouteData = Microsoft.AspNetCore.Routing.RouteData;

namespace DFC.App.MatchSkills.Test.Service
{
    public class SessionRequiredAttributeTests
    {
        private ISessionService _sessionService;
        private ICookieService _cookieService;
        private Mock<IServiceProvider> _serviceProvider;
        
        [SetUp]
        public void Setup()
        {
            _sessionService = Substitute.For<ISessionService>();
            _cookieService = Substitute.For<ICookieService>();

            _serviceProvider  = new Mock<IServiceProvider>();
            _serviceProvider
                .Setup(x => x.GetService(typeof(ISessionService)))
                .Returns(_sessionService);
            _serviceProvider
                .Setup(x => x.GetService(typeof(ICookieService)))
                .Returns(_cookieService);

            var serviceScope = new Mock<IServiceScope>();
            serviceScope.Setup(x => x.ServiceProvider).Returns(_serviceProvider.Object);

            var serviceScopeFactory = new Mock<IServiceScopeFactory>();
            serviceScopeFactory
                .Setup(x => x.CreateScope())
                .Returns(serviceScope.Object);

            _serviceProvider
                .Setup(x => x.GetService(typeof(IServiceScopeFactory)))
                .Returns(serviceScopeFactory.Object);
        }

        [Test]
        public async Task When_SessionFound_ThenNoErrorThrown()
        {
            var httpContext = Substitute.For<HttpContext>();
            var actionContext = new ActionContext(httpContext
                , Substitute.For<RouteData>(), Substitute.For<ActionDescriptor>());
            var context = new ActionExecutingContext(actionContext, new List<IFilterMetadata>(),
                new Dictionary<string, object>(), Substitute.For<Controller>());
            _sessionService.GetUserSession(Arg.Any<string>()).ReturnsForAnyArgs(new UserSession());
            _cookieService.TryGetPrimaryKey(Arg.Any<HttpRequest>(), Arg.Any<HttpResponse>()).Returns("test");
            httpContext.RequestServices = _serviceProvider.Object;

            var filter = new SessionRequiredAttribute();
            Func<Task> act = async () =>
            {
                await filter.OnActionExecutionAsync(context, Substitute.For<ActionExecutionDelegate>());
            };

            await act.Should().NotThrowAsync();
        }


        [Test]
        public async Task When_CookieIsNull_ThenErrorThrown()
        {
            var httpContext = Substitute.For<HttpContext>();
            var actionContext = new ActionContext(httpContext
                , Substitute.For<RouteData>(),Substitute.For<ActionDescriptor>());
            var context = new ActionExecutingContext(actionContext, new List<IFilterMetadata>(),
                new Dictionary<string, object>(), Substitute.For<Controller>());

            httpContext.RequestServices = _serviceProvider.Object;

            var filter = new SessionRequiredAttribute();
            Func<Task> act = async () =>
            {
                await filter.OnActionExecutionAsync(context, Substitute.For<ActionExecutionDelegate>());
            };

            await act.Should().ThrowAsync<Exception>();
        }

        [Test]
        public async Task When_SessionIsNull_ThenErrorThrown()
        {
            var httpContext = Substitute.For<HttpContext>();
            var actionContext = new ActionContext(httpContext
                , Substitute.For<RouteData>(), Substitute.For<ActionDescriptor>());
            var context = new ActionExecutingContext(actionContext, new List<IFilterMetadata>(),
                new Dictionary<string, object>(), Substitute.For<Controller>());

            httpContext.RequestServices = _serviceProvider.Object;

            var filter = new SessionRequiredAttribute();
            Func<Task> act = async () =>
            {
                await filter.OnActionExecutionAsync(context, Substitute.For<ActionExecutionDelegate>());
            };

            await act.Should().ThrowAsync<Exception>();
        }
    }
}
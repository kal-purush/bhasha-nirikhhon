using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using SFA.DAS.RoATPService.Api.Types.Models;
using SFA.DAS.RoATPService.Application.Handlers;
using SFA.DAS.RoATPService.Application.Interfaces;
using SFA.DAS.RoATPService.Domain;

namespace SFA.DAS.RoATPService.Application.UnitTests
{
    [TestFixture]
    public class GetEngagementsHandlerTests
    {
        private GetEngagementsHandler _handler;
        private Mock<IOrganisationRepository> _repository;
        private Mock<ILogger<GetEngagementsHandler>> _logger;

        [SetUp]
        public void Before_each_test()
        {
            _logger = new Mock<ILogger<GetEngagementsHandler>>();
            _repository = new Mock<IOrganisationRepository>();
            var engagements = new List<Engagement>
            {
                new Engagement {ProviderId = 11111111, Event = "INITIATED", CreatedOn = DateTime.Today},
                new Engagement {ProviderId = 11111112, Event = "INITIATED", CreatedOn = DateTime.Today.AddDays(-1)},
                new Engagement {ProviderId = 11111113, Event = "INITIATED", CreatedOn = DateTime.Today.AddDays(-2)}
            };
            _repository.Setup(x => x.GetEngagements()).ReturnsAsync(engagements);
            _handler = new GetEngagementsHandler(_repository.Object, _logger.Object);
        }

        [Test]
        public void Handler_returns_list_of_engagements()
        {
            var engagements = _handler.Handle(new GetEngagementsRequest(), new CancellationToken()).Result;

            engagements.Should().NotBeNullOrEmpty();
        }

        [Test]
        public void Handler_returns_exception_from_repository()
        {
            _repository.Setup(x => x.GetEngagements())
                .Throws(new Exception("Unit test exception"));

            Func<Task> result = async () => await
                _handler.Handle(new GetEngagementsRequest(), new CancellationToken());
            result.Should().Throw<ApplicationException>();
        }
    }
}
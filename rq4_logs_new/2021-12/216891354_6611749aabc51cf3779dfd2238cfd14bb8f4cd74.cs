namespace EstateManagement.BusinessLogic.Tests.EventHandling
{
    using System.Threading;
    using System.Threading.Tasks;
    using BusinessLogic.EventHandling;
    using MediatR;
    using Moq;
    using Shouldly;
    using Testing;
    using Xunit;

    public class StatementDomainEventHandlerTests
    {
        [Fact]
        public async Task StatementDomainEventHandler_Handle_StatementGeneratedEvent_EventIsHandled()
        {
            Mock<IMediator> mediator = new Mock<IMediator>(MockBehavior.Strict);
            mediator.Setup(m => m.Send(It.IsAny<IRequest<Unit>>(), It.IsAny<CancellationToken>())).ReturnsAsync(new Unit());
            StatementDomainEventHandler handler = new StatementDomainEventHandler(mediator.Object);

            Should.NotThrow(async () =>
                            {
                                await handler.Handle(TestData.StatementGeneratedEvent, CancellationToken.None);
                            });
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EstateManagement.BusinessLogic.Tests.EventHandling
{
    using System.Threading;
    using BusinessLogic.EventHandling;
    using BusinessLogic.Services;
    using MediatR;
    using Moq;
    using Requests;
    using Shouldly;
    using Testing;
    using TransactionProcessor.Transaction.DomainEvents;
    using Xunit;

    public class TransactionDomainEventHandlerTests
    {
        [Fact]
        public async Task TransactionDomainEventHandler_Handle_TransactionHasBeenCompletedEvent_EventIsHandled()
        {
            Mock<IMediator> mediator = new Mock<IMediator>(MockBehavior.Strict);
            mediator.Setup(m => m.Send(It.IsAny<IRequest<Unit>>(), It.IsAny<CancellationToken>())).ReturnsAsync(new Unit());
            TransactionDomainEventHandler handler = new TransactionDomainEventHandler(mediator.Object);

            Should.NotThrow(async () =>
                            {
                                await handler.Handle(TestData.TransactionHasBeenCompletedEvent, CancellationToken.None);
                            });
        }
    }
}
using System;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using SFA.DAS.EmployerFinance.Messages.Events;
using SFA.DAS.Reservations.Application.AccountLegalEntities.Handlers;
using SFA.DAS.Reservations.Domain.Accounts;

namespace SFA.DAS.Reservations.Application.UnitTests.AccountLegalEntities.Handlers;

public class WhenHandlingLevyAddedEventNew
{
    private Mock<IAccountsService> _service;
    private LevyAddedToAccountEventHandler _handler;

    [SetUp]
    public void Arrange()
    {
        _service = new Mock<IAccountsService>();
        _handler = new LevyAddedToAccountEventHandler(_service.Object, Mock.Of<ILogger<LevyAddedToAccountEventHandler>>());
    }

    [Test]
    public async Task Then_The_Service_Is_Called_To_Update_The_Entity()
    {
        //Arrange
        var levyAddedToAccountEvent = new LevyAddedToAccount
        {
            AccountId= 5, 
            Amount = 100,
            Created = DateTime.Now
        };

        //Act
        await _handler.Handle(levyAddedToAccountEvent);

        //Assert
        _service.Verify(x => x.UpdateLevyStatus(levyAddedToAccountEvent.AccountId, true), Times.Once);
    }

    [Test]
    public void Then_Will_Throw_Exception_If_Signing_Agreement_And_Database_Update_Fails()
    {
        //Arrange
        var levyAddedToAccountEvent = new LevyAddedToAccount
        {
            AccountId= 5, 
            Amount = 100,
            Created = DateTime.Now
        };

        _service.Setup(x => x.UpdateLevyStatus(It.IsAny<long>(), It.IsAny<bool>()))
            .ThrowsAsync(new DbUpdateException("Failed", (Exception)null));

        //Act + Assert
        var action = () => _handler.Handle(levyAddedToAccountEvent);
        action.Should().ThrowAsync<DbUpdateException>();
    }
}
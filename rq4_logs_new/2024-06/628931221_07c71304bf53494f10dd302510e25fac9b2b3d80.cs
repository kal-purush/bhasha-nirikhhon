using System.Collections.Generic;
using System.Linq;
using HerPortal.BusinessLogic.Models;
using HerPortal.ManagementShell;
using Moq;
using NUnit.Framework;
using Tests.Builders;

namespace Tests.ManagementShell;

public class MigrateAdminsCommandTests
{
    private Mock<IDatabaseOperation> mockDatabaseOperation;
    private Mock<IOutputProvider> mockOutputProvider;
    private CommandHandler underTest;

    [SetUp]
    public void Setup()
    {
        mockOutputProvider = new Mock<IOutputProvider>();
        mockDatabaseOperation = new Mock<IDatabaseOperation>();
        var adminAction = new AdminAction(mockDatabaseOperation.Object);
        underTest = new CommandHandler(adminAction, mockOutputProvider.Object);
    }

    // Cheshire East C_0008 contains only two LAs:
    // - Cheshire East 660
    // - Cheshire West and Chester 665

    [Test]
    public void MigrateAdmins_IfOwnsAllLas_RemovesLasAndAddConsortia()
    {
        // Arrange
        var (user, localAuthorities, expectedConsortia) = SetupUser(
            new List<string> { "660", "665" },
            new List<string>(),
            new List<string> { "C_0008" });

        // Act
        underTest.MigrateAdmins();

        // Assert
        mockDatabaseOperation.Verify(
            db => db.AddConsortiaAndRemoveLasFromUser(user, expectedConsortia, localAuthorities));
    }

    [Test]
    public void MigrateAdmins_IfOwnsNotEnoughLas_DoesNothing()
    {
        // Arrange
        var (user, _, _) = SetupUser(
            new List<string> { "660" },
            new List<string>(),
            new List<string> { "C_0008" });

        // Act
        underTest.MigrateAdmins();

        // Assert
        mockDatabaseOperation.Verify(
            db => db.AddConsortiaAndRemoveLasFromUser(
                user, It.IsAny<List<Consortium>>(), It.IsAny<List<LocalAuthority>>()),
            Times.Never);

        mockDatabaseOperation.VerifyNoOtherCalls();
    }

    [Test]
    public void MigrateAdmins_IfOwnsAllLasInTwoConsortia_AddsBoth()
    {
        // Arrange
        var (user, localAuthorities, expectedConsortia) = SetupUser(
            new List<string> { "660", "665", "835", "840" },
            new List<string>(),
            new List<string> { "C_0008", "C_0010" });

        // Act
        underTest.MigrateAdmins();

        // Assert
        mockDatabaseOperation.Verify(
            db => db.AddConsortiaAndRemoveLasFromUser(user, expectedConsortia, localAuthorities));

        mockDatabaseOperation.VerifyNoOtherCalls();
    }

    [Test]
    public void MigrateAdmins_IfOwnsAllInAConsortiaButNotEnoughInAnother_AddsOneConsortia()
    {
        // Arrange
        var (user, localAuthorities, expectedConsortia) = SetupUser(
            new List<string> { "660", "665", "835" },
            new List<string>(),
            new List<string> { "C_0008" });

        // Act
        underTest.MigrateAdmins();

        // Assert
        mockDatabaseOperation.Verify(
            db => db.AddConsortiaAndRemoveLasFromUser(user, expectedConsortia, localAuthorities));

        mockDatabaseOperation.VerifyNoOtherCalls();
    }

    [Test]
    public void MigrateAdmins_IfOwnsConsortia_DoesNothing()
    {
        // Arrange
        var (user, _, _) = SetupUser(
            new List<string>(),
            new List<string> { "C_0008" },
            new List<string> { "C_0008" });

        // Act
        underTest.MigrateAdmins();

        // Assert
        mockDatabaseOperation.Verify(
            db => db.AddConsortiaAndRemoveLasFromUser(
                user, It.IsAny<List<Consortium>>(), It.IsAny<List<LocalAuthority>>()),
            Times.Never);

        mockDatabaseOperation.VerifyNoOtherCalls();
    }

    [Test]
    public void MigrateAdmins_IfOwnsAllLasAndAlreadyOwnsConsortia_RemovesLasButLeavesConsortia()
    {
        // Arrange
        var (user, localAuthorities, _) = SetupUser(
            new List<string> { "835", "840" },
            new List<string> { "C_0008" },
            new List<string> { "C_0008" });

        // Act
        underTest.MigrateAdmins();

        // Assert
        mockDatabaseOperation.Verify(
            db => db.RemoveLasFromUser(user, localAuthorities));

        mockDatabaseOperation.VerifyNoOtherCalls();
    }

    private (User, List<LocalAuthority>, List<Consortium>) SetupUser(IEnumerable<string> custodianCodes,
        IReadOnlyCollection<string> consortiumCodes, IReadOnlyCollection<string> expectedConsortiumCodes)
    {
        var localAuthorities = custodianCodes
            .Select((custodianCode, i) => new LocalAuthority
            {
                Id = i,
                CustodianCode = custodianCode
            })
            .ToList();
        var consortia = consortiumCodes
            .Select((consortiumCode, i) => new Consortium
            {
                Id = i,
                ConsortiumCode = consortiumCode
            })
            .ToList();
        var expectedConsortia = expectedConsortiumCodes
            .Select((consortiumCode, i) => new Consortium
            {
                Id = i + consortiumCodes.Count,
                ConsortiumCode = consortiumCode
            })
            .ToList();

        var user = new UserBuilder("user@example.com")
            .WithLocalAuthorities(localAuthorities)
            .WithConsortia(consortia)
            .Build();
        mockDatabaseOperation
            .Setup(db => db.GetUsersWithLocalAuthoritiesAndConsortia())
            .Returns(new List<User> { user });
        mockDatabaseOperation
            .Setup(db => db.GetConsortia(expectedConsortiumCodes))
            .Returns(expectedConsortia);

        return (user, localAuthorities, expectedConsortia);
    }
}
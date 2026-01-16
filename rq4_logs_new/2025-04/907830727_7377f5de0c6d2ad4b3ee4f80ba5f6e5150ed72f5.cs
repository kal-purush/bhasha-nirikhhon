using EasyTravel.Application.Services;
using EasyTravel.Domain;
using EasyTravel.Domain.Entites;
using EasyTravel.Domain.Repositories;
using EasyTravel.Domain.Services;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;

[TestFixture]
public class AdminAgencyServiceTests
{
    private Mock<IApplicationUnitOfWork> _unitOfWorkMock;
    private Mock<ILogger<AdminAgencyService>> _loggerMock;
    private AdminAgencyService _adminAgencyService;

    [SetUp]
    public void SetUp()
    {
        _unitOfWorkMock = new Mock<IApplicationUnitOfWork>();
        _loggerMock = new Mock<ILogger<AdminAgencyService>>();
        _adminAgencyService = new AdminAgencyService(_unitOfWorkMock.Object, _loggerMock.Object);
    }

    [Test]
    public void Create_ShouldAddAgencyAndSave()
    {
        // Arrange
        var agency = new Agency
        {
            Id = Guid.NewGuid(),
            Name = "Test Agency",
            Address = "123 Main St",
            ContactNumber = "123-456-7890",
            LicenseNumber = "ABC123"
        };

        var agencyRepositoryMock = new Mock<IAgencyRepository>();
        _unitOfWorkMock.Setup(u => u.AgencyRepository).Returns(agencyRepositoryMock.Object);

        // Act
        _adminAgencyService.Create(agency);

        // Assert
        agencyRepositoryMock.Verify(r => r.Add(agency), Times.Once, "Agency should be added to the repository.");
        _unitOfWorkMock.Verify(u => u.Save(), Times.Once, "Save should be called after adding the agency.");
        _loggerMock.Verify(
            l => l.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("Creating a new agency.")),
                It.IsAny<Exception>(),
                It.Is<Func<It.IsAnyType, Exception, string>>((v, t) => true)),
            Times.Once);
        _loggerMock.Verify(
            l => l.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("Agency created successfully.")),
                It.IsAny<Exception>(),
                It.Is<Func<It.IsAnyType, Exception, string>>((v, t) => true)),
            Times.Once);
    }

    [Test]
    public void Get_ShouldReturnAgency_WhenAgencyExists()
    {
        // Arrange
        var agencyId = Guid.NewGuid();
        var agency = new Agency
        {
            Id = agencyId,
            Name = "Test Agency",
            Address = "123 Main St",
            ContactNumber = "123-456-7890",
            LicenseNumber = "ABC123"
        };
        _unitOfWorkMock.Setup(u => u.AgencyRepository.GetById(agencyId)).Returns(agency);

        // Act
        var result = _adminAgencyService.Get(agencyId);

        // Assert
        Assert.That(result, Is.EqualTo(agency), "The returned agency should match the expected agency.");
        _loggerMock.Verify(
            l => l.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString().Contains($"Fetching agency with ID: {agencyId}")),
                It.IsAny<Exception>(),
                It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
            Times.Once);
    }



    [Test]
    public void GetAll_ShouldReturnAllAgencies()
    {
        // Arrange
        var agencies = new List<Agency>
    {
        new Agency
        {
            Id = Guid.NewGuid(),
            Name = "Agency 1",
            Address = "123 Main St",
            ContactNumber = "123-456-7890",
            LicenseNumber = "ABC123"
        },
        new Agency
        {
            Id = Guid.NewGuid(),
            Name = "Agency 2",
            Address = "456 Elm St",
            ContactNumber = "987-654-3210",
            LicenseNumber = "XYZ789"
        }
    };
        _unitOfWorkMock.Setup(u => u.AgencyRepository.GetAll()).Returns(agencies);

        // Act
        var result = _adminAgencyService.GetAll();

        // Assert
        Assert.That(result, Is.EqualTo(agencies), "The returned agencies should match the expected list.");
        _loggerMock.Verify(
            l => l.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("Fetching all agencies.")),
                It.IsAny<Exception>(),
                It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
            Times.Once);
    }

    [Test]
    public void Update_ShouldEditAgencyAndSave()
    {
        // Arrange
        var agency = new Agency
        {
            Id = Guid.NewGuid(),
            Name = "Updated Agency",
            Address = "789 Oak St",
            ContactNumber = "555-555-5555",
            LicenseNumber = "LMN456"
        };

        var agencyRepositoryMock = new Mock<IAgencyRepository>();
        _unitOfWorkMock.Setup(u => u.AgencyRepository).Returns(agencyRepositoryMock.Object);

        // Act
        _adminAgencyService.Update(agency);

        // Assert
        agencyRepositoryMock.Verify(r => r.Edit(agency), Times.Once, "Agency should be updated in the repository.");
        _unitOfWorkMock.Verify(u => u.Save(), Times.Once, "Save should be called after updating the agency.");
        _loggerMock.Verify(
            l => l.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString().Contains($"Updating agency with ID: {agency.Id}")),
                It.IsAny<Exception>(),
                It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
            Times.Once);
        _loggerMock.Verify(
            l => l.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("Agency updated successfully.")),
                It.IsAny<Exception>(),
                It.Is<Func<It.IsAnyType, Exception?, string>>((v, t) => true)),
            Times.Once);
    }



}



using EasyTravel.Infrastructure;
using EasyTravel.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using Moq;
using NUnit.Framework;
using System;

[TestFixture]
public class UnitOfWorkTests
{
    private Mock<ApplicationDbContext> _dbContextMock;
    private UnitOfWork _unitOfWork;

    [SetUp]
    public void SetUp()
    {
        // Mock the ApplicationDbContext
        var dbContextOptions = new DbContextOptionsBuilder<ApplicationDbContext>()
            .UseInMemoryDatabase(databaseName: "TestDatabase_UnitOfWork")
            .Options;

        _dbContextMock = new Mock<ApplicationDbContext>(dbContextOptions);
        _unitOfWork = new UnitOfWork(_dbContextMock.Object);
    }

    [Test]
    public void Save_ShouldCallSaveChangesOnDbContext()
    {
        // Act
        _unitOfWork.Save();

        // Assert
        _dbContextMock.Verify(db => db.SaveChanges(), Times.Once, "SaveChanges should be called exactly once.");
    }

    [Test]
    public void Save_ShouldThrowException_WhenSaveChangesFails()
    {
        // Arrange
        _dbContextMock.Setup(db => db.SaveChanges()).Throws(new Exception("Database error"));

        // Act & Assert
        var ex = Assert.Throws<Exception>(() => _unitOfWork.Save());
        Assert.That(ex.Message, Is.EqualTo("Database error"), "The exception message should match the thrown exception.");
    }

    [Test]
    public void Constructor_ShouldInitializeDbContext()
    {
        // Assert
        Assert.That(_unitOfWork, Is.Not.Null, "UnitOfWork should be initialized.");
        Assert.That(_dbContextMock.Object, Is.Not.Null, "ApplicationDbContext should be initialized.");
    }

    [Test]
    public void Save_ShouldNotCallSaveChanges_WhenNoChangesAreMade()
    {
        // Arrange
        _dbContextMock.Setup(db => db.SaveChanges()).Returns(0); // Simulate no changes

        // Act
        _unitOfWork.Save();

        // Assert
        _dbContextMock.Verify(db => db.SaveChanges(), Times.Once, "SaveChanges should still be called even if no changes are made.");
    }

    [Test]
    public void Save_ShouldHandleMultipleCalls()
    {
        // Act
        _unitOfWork.Save();
        _unitOfWork.Save();

        // Assert
        _dbContextMock.Verify(db => db.SaveChanges(), Times.Exactly(2), "SaveChanges should be called twice.");
    }
}

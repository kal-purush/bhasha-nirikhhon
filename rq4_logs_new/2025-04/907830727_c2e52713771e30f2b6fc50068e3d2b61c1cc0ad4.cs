using EasyTravel.Domain.Entites;
using EasyTravel.Infrastructure.Data;
using EasyTravel.Infrastructure.Repositories;
using Microsoft.EntityFrameworkCore;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore.InMemory; // Add this using directive

[TestFixture]
public class AgencyRepositoryTests
{
    private ApplicationDbContext _context;
    private AgencyRepository _repository;

    [SetUp]
    public void SetUp()
    {
        // Use an in-memory database for testing
        var options = new DbContextOptionsBuilder<ApplicationDbContext>()
            .UseInMemoryDatabase(databaseName: "TestDatabase")
            .Options;

        // Use the new constructor for testing
        _context = new ApplicationDbContext(options);
        _repository = new AgencyRepository(_context);

        // Seed data
        _context.Agencies.AddRange(new List<Agency>
    {
        new Agency
        {
            Id = Guid.NewGuid(),
            Name = "Agency 1",
            Address = "123 Main St, New York, NY",
            ContactNumber = "123-456-7890",
            LicenseNumber = "LICENSE123"
        },
        new Agency
        {
            Id = Guid.NewGuid(),
            Name = "Agency 2",
            Address = "456 Elm St, Los Angeles, CA",
            ContactNumber = "987-654-3210",
            LicenseNumber = "LICENSE456"
        }
    });
        _context.SaveChanges();
    }



    [TearDown]
    public void TearDown()
    {
        // Clean up the in-memory database after each test
        _context.Database.EnsureDeleted();
        _context.Dispose();
    }

    [Test]
    public void Add_ShouldAddAgency()
    {
        // Arrange
        var agency = new Agency
        {
            Id = Guid.NewGuid(),
            Name = "Test Agency",
            Address = "789 Oak St, Chicago, IL",
            ContactNumber = "654-321-0987",
            LicenseNumber = "LICENSE789"
        };

        // Act
        _repository.Add(agency);
        _context.SaveChanges();

        // Assert
        var result = _repository.GetAll();
        Assert.That(result.Count(), Is.EqualTo(3)); // 2 seeded + 1 added
        Assert.That(result.Any(a => a.Name == "Test Agency"), Is.True);
    }

    [Test]
    public void GetAll_ShouldReturnAllAgencies()
    {
        // Act
        var result = _repository.GetAll();

        // Assert
        Assert.That(result.Count(), Is.EqualTo(2)); // 2 seeded agencies
        Assert.That(result.First().Name, Is.EqualTo("Agency 1"));
    }

    [Test]
    public void Remove_ShouldRemoveAgency()
    {
        // Arrange
        var agencyToRemove = _context.Agencies.First();

        // Act
        _repository.Remove(agencyToRemove);
        _context.SaveChanges();

        // Assert
        var result = _repository.GetAll();
        Assert.That(result.Count(), Is.EqualTo(1)); // 1 remaining after removal
        Assert.That(result.Any(a => a.Id == agencyToRemove.Id), Is.False);
    }

    [Test]
    public void Edit_ShouldUpdateAgency()
    {
        // Arrange
        var agencyToEdit = _context.Agencies.First();
        agencyToEdit.Name = "Updated Agency";

        // Act
        _repository.Edit(agencyToEdit);
        _context.SaveChanges();

        // Assert
        var updatedAgency = _repository.GetAll().First(a => a.Id == agencyToEdit.Id);
        Assert.That(updatedAgency.Name, Is.EqualTo("Updated Agency"));
    }
}
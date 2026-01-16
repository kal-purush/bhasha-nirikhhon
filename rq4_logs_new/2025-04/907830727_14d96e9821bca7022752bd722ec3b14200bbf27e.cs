using EasyTravel.Infrastructure.Data;
using EasyTravel.Domain.Entites;
using Microsoft.EntityFrameworkCore;
using NUnit.Framework;
using System;
using System.Linq;

[TestFixture]
public class ApplicationDbContextTests
{
    private ApplicationDbContext _context;

    [SetUp]
    public void SetUp()
    {
        var options = new DbContextOptionsBuilder<ApplicationDbContext>()
            .UseInMemoryDatabase(databaseName: "TestDatabase_ApplicationDbContext")
            .Options;

        _context = new ApplicationDbContext(options);

        // Seed data
        _context.Database.EnsureCreated();
    }

    [TearDown]
    public void TearDown()
    {
        _context.Database.EnsureDeleted();
        _context.Dispose();
    }

    [Test]
    public void ShouldSeedDefaultRoles()
    {
        // Act
        var roles = _context.Roles.ToList();

        // Assert
        Assert.That(roles.Count, Is.EqualTo(6));
        Assert.That(roles.Any(r => r.Name == "admin"), Is.True);
        Assert.That(roles.Any(r => r.Name == "client"), Is.True);
        Assert.That(roles.Any(r => r.Name == "agencyManager"), Is.True);
        Assert.That(roles.Any(r => r.Name == "busManager"), Is.True);
        Assert.That(roles.Any(r => r.Name == "carManager"), Is.True);
        Assert.That(roles.Any(r => r.Name == "hotelManager"), Is.True);
    }

    [Test]
    public void ShouldSeedDefaultAgencies()
    {
        // Act
        var agencies = _context.Agencies.ToList();

        // Assert
        Assert.That(agencies.Count, Is.EqualTo(1));
        Assert.That(agencies.First().Name, Is.EqualTo("Irfan"));
    }

    [Test]
    public void ShouldSeedDefaultPhotographers()
    {
        // Act
        var photographers = _context.Photographers.ToList();

        // Assert
        Assert.That(photographers.Count, Is.EqualTo(1));
        Assert.That(photographers.First().FirstName, Is.EqualTo("Irfan"));
    }

    [Test]
    public void ShouldSeedDefaultGuides()
    {
        // Act
        var guides = _context.Guides.ToList();

        // Assert
        Assert.That(guides.Count, Is.EqualTo(1));
        Assert.That(guides.First().FirstName, Is.EqualTo("Irfan"));
    }

    [Test]
    public void ShouldSeedDefaultHotels()
    {
        // Act
        var hotels = _context.Hotels.ToList();

        // Assert
        Assert.That(hotels.Count, Is.EqualTo(2));
        Assert.That(hotels.Any(h => h.Name == "Grand Hotel"), Is.True);
        Assert.That(hotels.Any(h => h.Name == "Sunset Resort"), Is.True);
    }



}

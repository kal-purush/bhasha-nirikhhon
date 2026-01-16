using EasyTravel.Infrastructure.Data;
using EasyTravel.Domain.Entites;
using EasyTravel.Infrastructure.Data;
using EasyTravel.Infrastructure.Repositories;
using Microsoft.EntityFrameworkCore;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;

[TestFixture]
public class BusRepositoryTests
{
    private ApplicationDbContext _context;
    private BusRepository _repository;

    [SetUp]
    public void SetUp()
    {
        var options = new DbContextOptionsBuilder<ApplicationDbContext>()
            .UseInMemoryDatabase(databaseName: "TestDatabase_Bus")
            .Options;

        _context = new ApplicationDbContext(options);
        _repository = new BusRepository(_context);

        // Seed data
        _context.Buses.AddRange(new List<Bus>
        {
            new Bus
            {
                Id = Guid.NewGuid(),
                OperatorName = "Operator 1",
                BusType = "Luxury",
                From = "City A",
                To = "City B",
                DepartureTime = DateTime.Now.AddHours(1),
                ArrivalTime = DateTime.Now.AddHours(5),
                Price = 50.00m,
                TotalSeats = 28
            },
            new Bus
            {
                Id = Guid.NewGuid(),
                OperatorName = "Operator 2",
                BusType = "Standard",
                From = "City C",
                To = "City D",
                DepartureTime = DateTime.Now.AddHours(2),
                ArrivalTime = DateTime.Now.AddHours(6),
                Price = 30.00m,
                TotalSeats = 28
            }
        });
        _context.SaveChanges();
    }

    [TearDown]
    public void TearDown()
    {
        _context.Database.EnsureDeleted();
        _context.Dispose();
    }

    [Test]
    public void GetAllBuses_ShouldReturnAllBuses()
    {
        var result = _repository.GetAllBuses();

        Assert.That(result.Count(), Is.EqualTo(2));
        Assert.That(result.First().OperatorName, Is.EqualTo("Operator 1"));
    }

    [Test]
    public void AddBus_ShouldAddBus()
    {
        var bus = new Bus
        {
            Id = Guid.NewGuid(),
            OperatorName = "Operator 3",
            BusType = "Mini",
            From = "City E",
            To = "City F",
            DepartureTime = DateTime.Now.AddHours(3),
            ArrivalTime = DateTime.Now.AddHours(7),
            Price = 40.00m,
            TotalSeats = 20
        };

        _repository.Addbus(bus);
        _context.SaveChanges();

        var result = _repository.GetAllBuses();
        Assert.That(result.Count(), Is.EqualTo(3));
        Assert.That(result.Any(b => b.OperatorName == "Operator 3"), Is.True);
    }
}
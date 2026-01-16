using EasyTravel.Domain.Entites;
using EasyTravel.Infrastructure.Data;
using EasyTravel.Infrastructure.Repositories;
using Microsoft.EntityFrameworkCore;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;

[TestFixture]
public class PhotographerBookingRepositoryTests
{
    private ApplicationDbContext _context;
    private PhotographerBookingRepository _repository;

    [SetUp]
    public void SetUp()
    {
        var options = new DbContextOptionsBuilder<ApplicationDbContext>()
            .UseInMemoryDatabase(databaseName: "TestDatabase_PhotographerBooking")
            .Options;

        _context = new ApplicationDbContext(options);
        _repository = new PhotographerBookingRepository(_context);

        // Seed data
        var photographer = new Photographer
        {
            Id = Guid.NewGuid(),
            FirstName = "John",
            LastName = "Doe",
            Email = "john.doe@example.com",
            ContactNumber = "123-456-7890",
            PreferredLocations = "City A, City B",
            PreferredEvents = "Weddings, Portraits",
            Address = "123 Main St",
            ProfilePicture = "profile1.jpg",
            Bio = "Experienced photographer specializing in weddings.",
            DateOfBirth = new DateTime(1985, 1, 1),
            Skills = "Photography, Editing",
            PortfolioUrl = "http://portfolio.com/johndoe",
            Specialization = "Weddings",
            YearsOfExperience = 10,
            Availability = true,
            HourlyRate = 50.00m,
            Rating = 4.5m,
            HireDate = DateTime.Now.AddYears(-3),
            UpdatedAt = DateTime.Now,
            AgencyId = Guid.NewGuid()
        };

        _context.Photographers.Add(photographer);

        _context.PhotographerBookings.AddRange(new List<PhotographerBooking>
        {
            new PhotographerBooking
            {
                Id = Guid.NewGuid(),
                UserName = "User1",
                Email = "user1@example.com",
                PhoneNumber = "111-222-3333",
                Gender = "Male",
                EventType = "Wedding",
                EventLocation = "City A",
                EventDate = DateTime.Now.AddDays(1),
                StartTime = new TimeSpan(9, 0, 0),
                EndTime = new TimeSpan(12, 0, 0),
                TimeInHour = 3,
                PhotographerId = photographer.Id
            },
            new PhotographerBooking
            {
                Id = Guid.NewGuid(),
                UserName = "User2",
                Email = "user2@example.com",
                PhoneNumber = "444-555-6666",
                Gender = "Female",
                EventType = "Portrait",
                EventLocation = "City B",
                EventDate = DateTime.Now.AddDays(2),
                StartTime = new TimeSpan(10, 0, 0),
                EndTime = new TimeSpan(14, 0, 0),
                TimeInHour = 4,
                PhotographerId = photographer.Id
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
    public void GetAllPhotographerBookings_ShouldReturnAllPhotographerBookings()
    {
        // Act
        var result = _repository.GetAll();

        // Assert
        Assert.That(result.Count(), Is.EqualTo(2));
        Assert.That(result.First().UserName, Is.EqualTo("User1"));
    }

    [Test]
    public void AddPhotographerBooking_ShouldAddPhotographerBooking()
    {
        // Arrange
        var photographerBooking = new PhotographerBooking
        {
            Id = Guid.NewGuid(),
            UserName = "User3",
            Email = "user3@example.com",
            PhoneNumber = "777-888-9999",
            Gender = "Non-Binary",
            EventType = "Event",
            EventLocation = "City C",
            EventDate = DateTime.Now.AddDays(3),
            StartTime = new TimeSpan(8, 0, 0),
            EndTime = new TimeSpan(11, 0, 0),
            TimeInHour = 3,
            PhotographerId = _context.Photographers.First().Id
        };

        // Act
        _repository.Add(photographerBooking);
        _context.SaveChanges();

        // Assert
        var result = _repository.GetAll();
        Assert.That(result.Count(), Is.EqualTo(3));
        Assert.That(result.Any(pb => pb.UserName == "User3"), Is.True);
    }
}
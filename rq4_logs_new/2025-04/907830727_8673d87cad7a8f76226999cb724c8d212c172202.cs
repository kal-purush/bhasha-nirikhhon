using EasyTravel.Domain.Entites;
using EasyTravel.Infrastructure.Data;
using EasyTravel.Infrastructure.Repositories;
using Microsoft.EntityFrameworkCore;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
[TestFixture]
public class GuideBookingRepositoryTests
{
    private ApplicationDbContext _context;
    private GuideBookingRepository _repository;

    [SetUp]
    public void SetUp()
    {
        var options = new DbContextOptionsBuilder<ApplicationDbContext>()
            .UseInMemoryDatabase(databaseName: "TestDatabase_GuideBooking")
            .Options;

        _context = new ApplicationDbContext(options);
        _repository = new GuideBookingRepository(_context);

        // Seed data
        var guide = new Guide
        {
            Id = Guid.NewGuid(),
            FirstName = "John",
            LastName = "Doe",
            Email = "john.doe@example.com",
            ContactNumber = "123-456-7890",
            Address = "123 Main St",
            ProfilePicture = "profile1.jpg",
            Bio = "Experienced guide",
            DateOfBirth = new DateTime(1985, 1, 1),
            LanguagesSpoken = "English, Spanish",
            PreferredLocations = "City A, City B",
            PreferredEvents = "City Tours",
            Specialization = "History",
            YearsOfExperience = 10,
            LicenseNumber = "LICENSE123",
            Availability = true,
            HourlyRate = 50.00m,
            Rating = 4.5m,
            AgencyId = Guid.NewGuid()
        };

        _context.Guides.Add(guide);

        _context.GuideBookings.AddRange(new List<GuideBooking>
        {
            new GuideBooking
            {
                Id = Guid.NewGuid(),
                UserName = "User1",
                Email = "user1@example.com",
                PhoneNumber = "111-222-3333",
                Gender = "Male",
                EventType = "City Tour",
                EventLocation = "City A",
                EventDate = DateTime.Now.AddDays(1),
                StartTime = new TimeSpan(9, 0, 0),
                EndTime = new TimeSpan(12, 0, 0),
                TimeInHour = 3,
                GuideId = guide.Id
            },
            new GuideBooking
            {
                Id = Guid.NewGuid(),
                UserName = "User2",
                Email = "user2@example.com",
                PhoneNumber = "444-555-6666",
                Gender = "Female",
                EventType = "Nature Tour",
                EventLocation = "City B",
                EventDate = DateTime.Now.AddDays(2),
                StartTime = new TimeSpan(10, 0, 0),
                EndTime = new TimeSpan(14, 0, 0),
                TimeInHour = 4,
                GuideId = guide.Id
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
    public void GetAllGuideBookings_ShouldReturnAllGuideBookings()
    {
        var result = _repository.GetAll();

        Assert.That(result.Count(), Is.EqualTo(2));
        Assert.That(result.First().UserName, Is.EqualTo("User1"));
    }

    [Test]
    public void AddGuideBooking_ShouldAddGuideBooking()
    {
        var guideBooking = new GuideBooking
        {
            Id = Guid.NewGuid(),
            UserName = "User3",
            Email = "user3@example.com",
            PhoneNumber = "777-888-9999",
            Gender = "Non-Binary",
            EventType = "Adventure Tour",
            EventLocation = "City C",
            EventDate = DateTime.Now.AddDays(3),
            StartTime = new TimeSpan(8, 0, 0),
            EndTime = new TimeSpan(11, 0, 0),
            TimeInHour = 3,
            GuideId = _context.Guides.First().Id
        };

        _repository.Add(guideBooking);
        _context.SaveChanges();

        var result = _repository.GetAll();
        Assert.That(result.Count(), Is.EqualTo(3));
        Assert.That(result.Any(gb => gb.UserName == "User3"), Is.True);
    }
}
using EasyTravel.Domain.Entites;
using EasyTravel.Infrastructure.Data;
using EasyTravel.Infrastructure.Repositories;
using Microsoft.EntityFrameworkCore;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
[TestFixture]
public class HotelBookingRepositoryTests
{
    private ApplicationDbContext _context;
    private HotelBookingRepository _repository;

    [SetUp]
    public void SetUp()
    {
        var options = new DbContextOptionsBuilder<ApplicationDbContext>()
            .UseInMemoryDatabase(databaseName: "TestDatabase_HotelBooking")
            .Options;

        _context = new ApplicationDbContext(options);
        _repository = new HotelBookingRepository(_context);

        // Seed data
        var hotel = new Hotel
        {
            Id = Guid.NewGuid(),
            Name = "Grand Hotel",
            Address = "123 Main St",
            City = "City A",
            Phone = "123-456-7890",
            Email = "info@grandhotel.com",
            Rating = 5,
            Image = "hotel.jpg",
            Description = "A luxurious hotel with modern amenities.",
            CreatedAt = DateTime.Now.AddDays(-10),
            UpdatedAt = DateTime.Now
        };

        _context.Hotels.Add(hotel);

        _context.HotelBookings.AddRange(new List<HotelBooking>
    {
        new HotelBooking
        {
            Id = Guid.NewGuid(),
            HotelId = hotel.Id,
            CheckInDate = DateTime.Now.AddDays(1),
            CheckOutDate = DateTime.Now.AddDays(5),
            RoomIdsJson = "[\"Room1\", \"Room2\"]"
        },
        new HotelBooking
        {
            Id = Guid.NewGuid(),
            HotelId = hotel.Id,
            CheckInDate = DateTime.Now.AddDays(2),
            CheckOutDate = DateTime.Now.AddDays(6),
            RoomIdsJson = "[\"Room3\", \"Room4\"]"
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
    public void GetAllHotelBookings_ShouldReturnAllHotelBookings()
    {
        var result = _repository.GetAll();

        Assert.That(result.Count(), Is.EqualTo(2));
        Assert.That(result.First().RoomIdsJson, Is.EqualTo("[\"Room1\", \"Room2\"]"));
    }

    [Test]
    public void AddHotelBooking_ShouldAddHotelBooking()
    {
        var hotelBooking = new HotelBooking
        {
            Id = Guid.NewGuid(),
            HotelId = _context.Hotels.First().Id,
            CheckInDate = DateTime.Now.AddDays(3),
            CheckOutDate = DateTime.Now.AddDays(7),
            RoomIdsJson = "[\"Room5\", \"Room6\"]"
        };

        _repository.Add(hotelBooking);
        _context.SaveChanges();

        var result = _repository.GetAll();
        Assert.That(result.Count(), Is.EqualTo(3));
        Assert.That(result.Any(hb => hb.RoomIdsJson == "[\"Room5\", \"Room6\"]"), Is.True);
    }
}
[TestFixture]
public class HotelRepositoryTests
{
    private ApplicationDbContext _context;
    private HotelRepository _repository;

    [SetUp]
    public void SetUp()
    {
        var options = new DbContextOptionsBuilder<ApplicationDbContext>()
            .UseInMemoryDatabase(databaseName: "TestDatabase_Hotel")
            .Options;

        _context = new ApplicationDbContext(options);
        _repository = new HotelRepository(_context);

        // Seed data
        _context.Hotels.AddRange(new List<Hotel>
        {
            new Hotel
            {
                Id = Guid.NewGuid(),
                Name = "Grand Hotel",
                Address = "123 Main St",
                City = "City A",
                Phone = "123-456-7890",
                Email = "info@grandhotel.com",
                Rating = 5,
                Image = "hotel1.jpg",
                 Description = "A beautiful resort with stunning sunset views.",
                CreatedAt = DateTime.Now.AddDays(-10),
                UpdatedAt = DateTime.Now
            },
            new Hotel
            {
                Id = Guid.NewGuid(),
                Name = "Sunset Resort",
                Address = "456 Beach Rd",
                City = "City B",
                Phone = "987-654-3210",
                Email = "info@sunsetresort.com",
                Rating = 4,
                Image = "hotel2.jpg",
                 Description = "A beautiful resort with stunning sunset views.",
                CreatedAt = DateTime.Now.AddDays(-20),
                UpdatedAt = DateTime.Now
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
    public void GetHotels_ShouldReturnHotelsByLocation()
    {
        var result = _repository.GetHotels("City A", null);

        Assert.That(result.Count(), Is.EqualTo(1));
        Assert.That(result.First().Name, Is.EqualTo("Grand Hotel"));
    }

    [Test]
    public void GetTopRatedAsync_ShouldReturnTopRatedHotels()
    {
        var result = _repository.GetTopRatedAsync();

        Assert.That(result.Count(), Is.EqualTo(2));
        Assert.That(result.First().Rating, Is.GreaterThanOrEqualTo(4));
    }
}
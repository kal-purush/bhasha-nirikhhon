using EasyTravel.Domain.Repositories;
using EasyTravel.Infrastructure;
using EasyTravel.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using Moq;
using NUnit.Framework;
using System;

[TestFixture]
public class ApplicationUnitOfWorkTests
{
    private ApplicationDbContext _context;
    private Mock<IBusRepository> _busRepositoryMock;
    private Mock<ICarRepository> _carRepositoryMock;
    private Mock<IAgencyRepository> _agencyRepositoryMock;
    private Mock<IPhotographerRepository> _photographerRepositoryMock;
    private Mock<IGuideRepository> _guideRepositoryMock;
    private Mock<IHotelRepository> _hotelRepositoryMock;
    private Mock<IRoomRepository> _roomRepositoryMock;
    private Mock<IHotelBookingRepository> _hotelBookingRepositoryMock;
    private Mock<IBusBookingRepository> _busBookingRepositoryMock;
    private Mock<ISeatRepository> _seatRepositoryMock;
    private Mock<ICarBookingRepository> _carBookingRepositoryMock;
    private Mock<IPhotographerBookingRepository> _photographerBookingRepositoryMock;
    private Mock<IGuideBookingRepository> _guideBookingRepositoryMock;
    private Mock<IBookingRepository> _bookingRepositoryMock;
    private Mock<IPaymentRepository> _paymentRepositoryMock;
    private Mock<IRecommendationRepository> _recommendationRepositoryMock;
    private ApplicationUnitOfWork _unitOfWork;

    [SetUp]
    public void SetUp()
    {
        var options = new DbContextOptionsBuilder<ApplicationDbContext>()
            .UseInMemoryDatabase(databaseName: "TestDatabase_ApplicationUnitOfWork")
            .Options;

        _context = new ApplicationDbContext(options);

        // Mock repositories
        _busRepositoryMock = new Mock<IBusRepository>();
        _carRepositoryMock = new Mock<ICarRepository>();
        _agencyRepositoryMock = new Mock<IAgencyRepository>();
        _photographerRepositoryMock = new Mock<IPhotographerRepository>();
        _guideRepositoryMock = new Mock<IGuideRepository>();
        _hotelRepositoryMock = new Mock<IHotelRepository>();
        _roomRepositoryMock = new Mock<IRoomRepository>();
        _hotelBookingRepositoryMock = new Mock<IHotelBookingRepository>();
        _busBookingRepositoryMock = new Mock<IBusBookingRepository>();
        _seatRepositoryMock = new Mock<ISeatRepository>();
        _carBookingRepositoryMock = new Mock<ICarBookingRepository>();
        _photographerBookingRepositoryMock = new Mock<IPhotographerBookingRepository>();
        _guideBookingRepositoryMock = new Mock<IGuideBookingRepository>();
        _bookingRepositoryMock = new Mock<IBookingRepository>();
        _paymentRepositoryMock = new Mock<IPaymentRepository>();
        _recommendationRepositoryMock = new Mock<IRecommendationRepository>();

        // Initialize ApplicationUnitOfWork
        _unitOfWork = new ApplicationUnitOfWork(
            _context,
            _busRepositoryMock.Object,
            _agencyRepositoryMock.Object,
            _photographerRepositoryMock.Object,
            _guideRepositoryMock.Object,
            _carRepositoryMock.Object,
            _hotelRepositoryMock.Object,
            _roomRepositoryMock.Object,
            _hotelBookingRepositoryMock.Object,
            _busBookingRepositoryMock.Object,
            _seatRepositoryMock.Object,
            _carBookingRepositoryMock.Object,
            _photographerBookingRepositoryMock.Object,
            _guideBookingRepositoryMock.Object,
            _bookingRepositoryMock.Object,
            _recommendationRepositoryMock.Object,
            _paymentRepositoryMock.Object
        );
    }

    [TearDown]
    public void TearDown()
    {
        _context.Database.EnsureDeleted();
        _context.Dispose();
    }

    [Test]
    public void ShouldInitializeAllRepositories()
    {
        // Assert
        Assert.That(_unitOfWork.BusRepository, Is.EqualTo(_busRepositoryMock.Object));
        Assert.That(_unitOfWork.CarRepository, Is.EqualTo(_carRepositoryMock.Object));
        Assert.That(_unitOfWork.AgencyRepository, Is.EqualTo(_agencyRepositoryMock.Object));
        Assert.That(_unitOfWork.PhotographerRepository, Is.EqualTo(_photographerRepositoryMock.Object));
        Assert.That(_unitOfWork.GuideRepository, Is.EqualTo(_guideRepositoryMock.Object));
        Assert.That(_unitOfWork.HotelRepository, Is.EqualTo(_hotelRepositoryMock.Object));
        Assert.That(_unitOfWork.RoomRepository, Is.EqualTo(_roomRepositoryMock.Object));
        Assert.That(_unitOfWork.HotelBookingRepository, Is.EqualTo(_hotelBookingRepositoryMock.Object));
        Assert.That(_unitOfWork.BusBookingRepository, Is.EqualTo(_busBookingRepositoryMock.Object));
        Assert.That(_unitOfWork.SeatRepository, Is.EqualTo(_seatRepositoryMock.Object));
        Assert.That(_unitOfWork.CarBookingRepository, Is.EqualTo(_carBookingRepositoryMock.Object));
        Assert.That(_unitOfWork.PhotographerBookingRepository, Is.EqualTo(_photographerBookingRepositoryMock.Object));
        Assert.That(_unitOfWork.GuideBookingRepository, Is.EqualTo(_guideBookingRepositoryMock.Object));
        Assert.That(_unitOfWork.BookingRepository, Is.EqualTo(_bookingRepositoryMock.Object));
        Assert.That(_unitOfWork.PaymentRepository, Is.EqualTo(_paymentRepositoryMock.Object));
        Assert.That(_unitOfWork.RecommendationRepository, Is.EqualTo(_recommendationRepositoryMock.Object));
    }


    [Test]
    public void Save_ShouldCallSaveChangesOnDbContext()
    {
        // Arrange
        var dbContextOptions = new DbContextOptionsBuilder<ApplicationDbContext>()
            .UseInMemoryDatabase(databaseName: "TestDatabase_ApplicationUnitOfWork_Save")
            .Options;

        var dbContextMock = new Mock<ApplicationDbContext>(dbContextOptions);
        var unitOfWork = new ApplicationUnitOfWork(
            dbContextMock.Object,
            _busRepositoryMock.Object,
            _agencyRepositoryMock.Object,
            _photographerRepositoryMock.Object,
            _guideRepositoryMock.Object,
            _carRepositoryMock.Object,
            _hotelRepositoryMock.Object,
            _roomRepositoryMock.Object,
            _hotelBookingRepositoryMock.Object,
            _busBookingRepositoryMock.Object,
            _seatRepositoryMock.Object,
            _carBookingRepositoryMock.Object,
            _photographerBookingRepositoryMock.Object,
            _guideBookingRepositoryMock.Object,
            _bookingRepositoryMock.Object,
            _recommendationRepositoryMock.Object,
            _paymentRepositoryMock.Object
        );

        // Act
        unitOfWork.Save();

        // Assert
        dbContextMock.Verify(c => c.SaveChanges(), Times.Once);
    }
  
}

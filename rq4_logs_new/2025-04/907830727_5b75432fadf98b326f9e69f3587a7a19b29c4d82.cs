using EasyTravel.Domain.Entites;
using EasyTravel.Domain.Services;
using EasyTravel.Web.Controllers;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;

namespace UnitTest
{
    [TestFixture]
    public class PaymentControllerTests
    {
        private Mock<IGetService<Booking, Guid>> _mockBookingService;
        private Mock<UserManager<User>> _mockUserManager;
        private Mock<ISessionService> _mockSessionService;
        private Mock<IPhotographerService> _mockPhotographerService;
        private Mock<IGuideService> _mockGuideService;
        private Mock<IBusService> _mockBusService;
        private Mock<ICarService> _mockCarService;
        private Mock<IHotelService> _mockHotelService;
        private Mock<IPhotographerBookingService> _mockPhotographerBookingService;
        private Mock<IGuideBookingService> _mockGuideBookingService;
        private Mock<ILogger<PaymentController>> _mockLogger;
        private Mock<IPaymentOnlyService> _mockPaymentOnlyService;
        private Mock<IConfiguration> _mockConfig;
        private Mock<IWebHostEnvironment> _mockEnv;
        private PaymentController _controller;

        [SetUp]
        public void SetUp()
        {
            _mockBookingService = new Mock<IGetService<Booking, Guid>>();
            _mockUserManager = new Mock<UserManager<User>>(
                Mock.Of<IUserStore<User>>(), null, null, null, null, null, null, null, null);
            _mockSessionService = new Mock<ISessionService>();
            _mockPhotographerService = new Mock<IPhotographerService>();
            _mockGuideService = new Mock<IGuideService>();
            _mockBusService = new Mock<IBusService>();
            _mockCarService = new Mock<ICarService>();
            _mockHotelService = new Mock<IHotelService>();
            _mockPhotographerBookingService = new Mock<IPhotographerBookingService>();
            _mockGuideBookingService = new Mock<IGuideBookingService>();
            _mockLogger = new Mock<ILogger<PaymentController>>();
            _mockPaymentOnlyService = new Mock<IPaymentOnlyService>();
            _mockConfig = new Mock<IConfiguration>();
            _mockEnv = new Mock<IWebHostEnvironment>();

            _controller = new PaymentController(
                _mockBookingService.Object,
                _mockUserManager.Object,
                _mockSessionService.Object,
                _mockPhotographerService.Object,
                _mockGuideService.Object,
                _mockBusService.Object,
                _mockCarService.Object,
                _mockHotelService.Object,
                _mockLogger.Object,
                _mockPhotographerBookingService.Object,
                _mockGuideBookingService.Object,
                _mockPaymentOnlyService.Object,
                _mockConfig.Object,
                _mockEnv.Object
            );
        }
        [TearDown]
        public void TearDown()
        {
            if (_controller is IDisposable disposableController)
            {
                disposableController.Dispose();
            }
        }
        [Test]
        public async Task Pay_ReturnsRedirectToExpired_WhenBookingAlreadyExists()
        {
            // Arrange
            var bookingId = Guid.NewGuid();
            var booking = new Booking { Id = bookingId, TotalAmount = 100, BookingTypes = BookingTypes.Bus };
            _mockBookingService.Setup(s => s.Get(bookingId)).Returns(booking);
            _mockPaymentOnlyService.Setup(s => s.IsExist(bookingId)).ReturnsAsync(true);

            // Act
            var result = await _controller.Pay(bookingId);

            // Assert
            Assert.That(result, Is.InstanceOf<RedirectToActionResult>());
            var redirectResult = result as RedirectToActionResult;
            Assert.That(redirectResult.ActionName, Is.EqualTo("Expired"));
        }


        [Test]
        public async Task Pay_ReturnsRedirectToGateway_WhenPaymentInitializedSuccessfully()
        {
            // Arrange
            var bookingId = Guid.NewGuid();
            var booking = new Booking { Id = bookingId, TotalAmount = 100, BookingTypes = BookingTypes.Bus };
            _mockBookingService.Setup(s => s.Get(bookingId)).Returns(booking);
            _mockPaymentOnlyService.Setup(s => s.IsExist(bookingId)).ReturnsAsync(false);
            _mockSessionService.Setup(s => s.GetString("TotalAmount")).Returns("100");
            _mockSessionService.Setup(s => s.GetString("BookingType")).Returns("Bus");

            var mockConfigSection = new Mock<IConfigurationSection>();
            mockConfigSection.Setup(c => c["StoreId"]).Returns("test_store_id");
            mockConfigSection.Setup(c => c["StorePassword"]).Returns("test_store_password");
            mockConfigSection.Setup(c => c["IsSandbox"]).Returns("true");

            _mockConfig.Setup(c => c.GetSection("SSLCommerz")).Returns(mockConfigSection.Object);

            // Act
            var result = await _controller.Pay(bookingId);

            // Assert
            Assert.That(result, Is.InstanceOf<RedirectToActionResult>());
        }
     

    }
}
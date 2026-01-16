using HOTELAPI1;
using HOTELAPI1.Controllers;
using HOTELAPI1.Models;
using HOTELAPI1.Services;
using Microsoft.AspNetCore.Mvc;
using Moq;
using System;
using System.Threading.Tasks;
using Xunit;
using HOTELAPI1.Abstract;
using Microsoft.EntityFrameworkCore;

namespace TestProject.ControllersTest
{
    public class PropiedadesControllerTests
    {
        [Fact]
        public async Task GetPropiedad_ReturnsOk_WhenPropiedadExists()
        {
            // Arrange
            var testId = Guid.NewGuid();
            var testPropiedad = new Propiedad { Id = testId, Nombre = "Test Propiedad" };

            var mockSet = new Mock<DbSet<Propiedad>>();

            // Configurar FindAsync para devolver la propiedad de prueba cuando se llama con el ID de prueba
            mockSet.Setup(m => m.FindAsync(testId))
                   .Returns(new ValueTask<Propiedad>(testPropiedad));

            var mockContext = new Mock<HotelDbContext>();
            mockContext.Setup(c => c.Propiedades).Returns(mockSet.Object);

            var service = new PropiedadService(mockContext.Object);
            var controller = new PropiedadesController(mockContext.Object, service);

            // Act
            var result = await controller.ObtenerPropiedadPorId(testId);

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(result);
            var returnValue = Assert.IsType<Propiedad>(okResult.Value);
            Assert.Equal("Test Propiedad", returnValue.Nombre);
        }



        [Fact]
        public async Task GetPropiedad_ReturnsNotFound_WhenPropiedadDoesNotExist()
        {
            // Arrange
            var testId = Guid.NewGuid();

            var mockService = new Mock<IPropiedadService>();
            mockService.Setup(svc => svc.GetPropiedadById(testId)).ReturnsAsync((Propiedad)null);

            var mockSet = new Mock<DbSet<Propiedad>>(); // Mocking DbSet
            var mockContext = new Mock<HotelDbContext>(); // Mocking DbContext
            mockContext.Setup(c => c.Propiedades).Returns(mockSet.Object); // Setup DbContext to return Mocked DbSet

            var controller = new PropiedadesController(mockContext.Object, mockService.Object);

            // Act
            var result = await controller.ObtenerPropiedadPorId(testId);

            // Assert
            Assert.IsType<NotFoundResult>(result);
        }


    }
}
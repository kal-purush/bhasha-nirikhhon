using HOTELAPI1.Abstract;
using HOTELAPI1;
using HOTELAPI1.Controllers;
using HOTELAPI1.Models;
using HOTELAPI1.Services;
using Microsoft.AspNetCore.Mvc;
using Moq;
using System;
using System.Threading.Tasks;
using Xunit;
using Microsoft.Extensions.Logging;

namespace TestProject.ControllersTest
{
    public class ClientesControllerTests
    {
        [Fact]
        public async Task GetCliente_ReturnsOk_WhenClienteExists()
        {
            // Arrange
            var testId = "testId";
            var testCliente = new Cliente { Id = testId, Nombre = "Test Cliente" };

            var mockService = new Mock<IClienteService>();
            mockService.Setup(svc => svc.GetClienteById(testId)).ReturnsAsync(testCliente);

            var mockContext = new Mock<HotelDbContext>();

            // Crear un mock del logger
            var mockLogger = new Mock<ILogger<ClientesController>>();

            // Ahora, pasa el mockLogger.Object al constructor del controlador
            var controller = new ClientesController(mockContext.Object, mockService.Object, mockLogger.Object);

            // Act
            try
            {
                var result = await controller.GetCliente(testId);

                var actionResult = Assert.IsType<ActionResult<Cliente>>(result);
                var okResult = Assert.IsType<OkObjectResult>(actionResult.Result);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex.Message}");
                throw; // Re-lanza la excepción para que la prueba falle y puedas ver el error.
            }
        }
        [Fact]
        public async Task GetCliente_ReturnsNotFound_WhenClienteDoesNotExist()
        {
            // Arrange
            var testId = "testId";

            var mockService = new Mock<IClienteService>();
            mockService.Setup(svc => svc.GetClienteById(testId)).ReturnsAsync((Cliente)null);

            var mockContext = new Mock<HotelDbContext>();

            // Crear un mock del logger
            var mockLogger = new Mock<ILogger<ClientesController>>();

            // Ahora, pasa el mockLogger.Object al constructor del controlador
            var controller = new ClientesController(mockContext.Object, mockService.Object, mockLogger.Object);

            try
            {
                // Act
                var result = await controller.GetCliente(testId);

                // Assert
                Assert.IsType<NotFoundResult>(result.Result);
            }
            catch (Exception ex)
            {

                throw;
            }

        }
    }
}
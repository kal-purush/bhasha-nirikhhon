using System.Security.Claims;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Moq;
using ms_spa.Api.Contract;
using ms_spa.Api.Contract.Cliente;
using ms_spa.Api.Controllers;
using ms_spa.Api.Domain.Services.Interfaces;
using ms_spa.Api.Exceptions;


namespace ms_spa.Test.Controller
{
    public class ClienteControllerTest
    {
        private readonly Mock<IService<ClienteRequestContract, ClienteResponseContract, int>> _clienteServiceMock = new();
        private readonly ClienteController _controller;

        public ClienteControllerTest()
        {
            _controller = new ClienteController(_clienteServiceMock.Object);

            var userClaims = new List<Claim>
            {
                new Claim(ClaimTypes.NameIdentifier, "1")
            };
            var identity = new ClaimsIdentity(userClaims, "TestAuthType");
            var clientePrincipal = new ClaimsPrincipal(identity);

            _controller.ControllerContext = new ControllerContext
            {
                HttpContext = new DefaultHttpContext { User = clientePrincipal }
            };
        }

        [Fact]
        public async Task Adicionar_DeveRetornarCreatedResult()
        {
            // Arrange
            _clienteServiceMock.Setup(service => service.Adicionar(It.IsAny<ClienteRequestContract>(), It.IsAny<int>()))
                .ReturnsAsync(new ClienteResponseContract());

            // Act
            var result = await _controller.Adicionar(new ClienteRequestContract());

            // Assert
            Assert.NotNull(result);
            Assert.IsType<CreatedResult>(result);
        }

        [Fact]
        public async Task Adicionar_DeveRetornarBadRequestQuandoFalha()
        {
            // Arrange
            var mensagemDeErro = "Mensagem de erro";
            _clienteServiceMock.Setup(service => service.Adicionar(It.IsAny<ClienteRequestContract>(), It.IsAny<int>()))
                               .ThrowsAsync(new BadRequestException(mensagemDeErro));

            // Act
            var result = await _controller.Adicionar(new ClienteRequestContract());

            // Assert
            Assert.NotNull(result);
            Assert.IsType<BadRequestObjectResult>(result);
            var badRequestResult = result as BadRequestObjectResult;
            Assert.Equal(400, badRequestResult?.StatusCode);
            var modelError = badRequestResult?.Value as ModelErrorContract;
            Assert.NotNull(modelError);
            Assert.Equal("Bad Request", modelError.Title);
            Assert.Equal(mensagemDeErro, modelError.Message);
        }

        [Fact]
        public async Task Atualizar_DeveRetornarOkResult()
        {
            // Arrange
            _clienteServiceMock.Setup(service => service.Atualizar(It.IsAny<int>(), It.IsAny<ClienteRequestContract>(), It.IsAny<int>()))
                               .ReturnsAsync(new ClienteResponseContract());

            // Act
            var result = await _controller.Atualizar(1, new ClienteRequestContract());

            // Assert
            Assert.IsType<OkObjectResult>(result);
        }

        [Fact]
        public async Task Atualizar_DeveRetornarNotFoundQuandoClienteNaoEncontrado()
        {
            // Arrange
            var clienteId = 1;
            var mensagemDeErro = "Cliente não encontrado";
            _clienteServiceMock.Setup(service => service.Atualizar(clienteId, It.IsAny<ClienteRequestContract>(), It.IsAny<int>()))
                               .ThrowsAsync(new NotFoundException(mensagemDeErro));

            // Act
            var result = await _controller.Atualizar(clienteId, new ClienteRequestContract());

            // Assert
            var notFoundResult = Assert.IsType<NotFoundObjectResult>(result);
            var modelError = Assert.IsType<ModelErrorContract>(notFoundResult.Value);
            Assert.Equal(404, notFoundResult.StatusCode);
            Assert.Equal("Not Found", modelError.Title);
            Assert.Equal(mensagemDeErro, modelError.Message);
        }

        [Fact]
        public async Task ObterPorId_DeveRetornarOkResult()
        {
            // Arrange
            var clienteId = 1;
            var clienteResponse = new ClienteResponseContract();
            _clienteServiceMock.Setup(service => service.ObterPorId(clienteId, It.IsAny<int>()))
                               .ReturnsAsync(clienteResponse);

            // Act
            var result = await _controller.ObterPorId(clienteId);

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(result);
            Assert.Equal(200, okResult.StatusCode);
            Assert.IsType<ClienteResponseContract>(okResult.Value);
        }

        [Fact]
        public async Task ObterPorId_DeveRetornarNotFoundQuandoClienteNaoEncontrado()
        {
            // Arrange
            var clienteId = 1;
            var mensagemDeErro = "Cliente não encontrado";
            _clienteServiceMock.Setup(service => service.ObterPorId(clienteId, It.IsAny<int>()))
                               .ThrowsAsync(new NotFoundException(mensagemDeErro));

            // Act
            var result = await _controller.ObterPorId(clienteId);

            // Assert
            var notFoundResult = Assert.IsType<NotFoundObjectResult>(result);
            var modelError = Assert.IsType<ModelErrorContract>(notFoundResult.Value);
            Assert.Equal(404, notFoundResult.StatusCode);
            Assert.Equal("Not Found", modelError.Title);
            Assert.Equal(mensagemDeErro, modelError.Message);
        }

        [Fact]
        public async Task ObterTodos_DeveRetornarOkResult()
        {
            // Arrange
            var clientesResponse = new List<ClienteResponseContract>();
            _clienteServiceMock.Setup(service => service.ObterTodos(It.IsAny<int>()))
                               .ReturnsAsync(clientesResponse);

            // Act
            var result = await _controller.ObterTodos();

            // Assert
            var okResult = Assert.IsType<OkObjectResult>(result);
            Assert.Equal(200, okResult.StatusCode);
            var responseBody = Assert.IsType<List<ClienteResponseContract>>(okResult.Value);
            Assert.Empty(responseBody);
        }

        [Fact]
        public async Task ObterTodos_DeveRetornarProblemaQuandoFalha()
        {
            // Arrange
            _clienteServiceMock.Setup(service => service.ObterTodos(It.IsAny<int>()))
                               .ThrowsAsync(new Exception("Erro ao obter clientes"));

            // Act
            var result = await _controller.ObterTodos();

            // Assert
            var objectResult = Assert.IsType<ObjectResult>(result);
            Assert.Equal(500, objectResult.StatusCode);
        }

        [Fact]
        public async Task Deletar_DeveRetornarNoContent()
        {
            // Arrange
            var clienteId = 1;
            _clienteServiceMock.Setup(service => service.Inativar(clienteId, It.IsAny<int>()))
                               .Returns(Task.CompletedTask);

            // Act
            var result = await _controller.Deletar(clienteId);

            // Assert
            Assert.IsType<NoContentResult>(result);
        }

        [Fact]
        public async Task Deletar_DeveRetornarNotFoundQuandoClienteNaoEncontrado()
        {
            // Arrange
            var clienteId = 1;
            var mensagemDeErro = "Cliente não encontrado";
            _clienteServiceMock.Setup(service => service.Inativar(clienteId, It.IsAny<int>()))
                               .ThrowsAsync(new NotFoundException(mensagemDeErro));

            // Act
            var result = await _controller.Deletar(clienteId);

            // Assert
            var notFoundResult = Assert.IsType<NotFoundObjectResult>(result);
            var modelError = Assert.IsType<ModelErrorContract>(notFoundResult.Value);
            Assert.Equal(404, notFoundResult.StatusCode);
            Assert.Equal("Not Found", modelError.Title);
            Assert.Equal(mensagemDeErro, modelError.Message);
        }
    }
}
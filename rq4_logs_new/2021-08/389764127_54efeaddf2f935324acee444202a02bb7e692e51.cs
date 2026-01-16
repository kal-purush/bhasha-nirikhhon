using System;
using System.Threading.Tasks;
using Api.Application.Controllers;
using Api.Domain.Dtos.CEP;
using Api.Domain.Interfaces.Services.Cep;
using Microsoft.AspNetCore.Mvc;
using Moq;
using Xunit;

namespace Api.Application.Test.Cep.QuandoRequisitarCreate
{
    public class Retorno_BadRequest
    {
        private CepsController _controller;
        
        [Fact(DisplayName = "É Possível Realizar o Created.")]
        public async Task E_Possivel_Invocar_a_Controller_Create()
        {
            var serviceMock = new Mock<ICepService>();
            serviceMock.Setup(m => m.Post(It.IsAny<CepDtoCreate>()))
            .ReturnsAsync(
                new CepDtoCreateResult
                {
                    Id = Guid.NewGuid(),
                    Logradouro = "Teste de Rua",
                    CreateAt = DateTime.UtcNow,
                }            
            );

            _controller = new CepsController(serviceMock.Object);
            _controller.ModelState.AddModelError("Nome", "É um Campo Obrigatório");

            Mock<IUrlHelper> url = new Mock<IUrlHelper>();
            url.Setup(x => x.Link(It.IsAny<string>(), It.IsAny<object>()))
            .Returns("http://localhost:5000");
            _controller.Url = url.Object;

            var CepDtoCreate = new CepDtoCreate
            {
                Logradouro = "Teste de Rua",
                Numero = ""
            };

            var result = await _controller.Post(CepDtoCreate);
            Assert.True(result is BadRequestObjectResult);
    }
}
}
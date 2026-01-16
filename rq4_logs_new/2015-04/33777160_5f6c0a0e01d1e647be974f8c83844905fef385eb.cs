using FluentAssertions;
using HeroisDoCancer.Models;
using HeroisDoCancer.Services;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Moq.Protected;
using System;

namespace HeroisDoCancer.Tests.Services
{
    [TestClass]
    public class VoluntarioServiceTest
    {
        [TestMethod]
        public void ComVoluntarioNoContextoDeveResgatarPeloNomeFabio()
        {
            var contextoMock = new Mock<IContextoDados>();
            contextoMock.SetupGet(c => c.Voluntarios)
                .Returns(new[]
                {
                    new VoluntarioModel { Nome = "Fabio" }
                });

            var servico = new VoluntarioService(contextoMock.Object);
            var voluntario = new VoluntarioModel { Nome = "Fabio" };

            servico.ExistePeloNome(voluntario)
                .Should()
                    .BeTrue();

            voluntario.Nome = "fabiO";
            servico.ExistePeloNome(voluntario)
                .Should()
                    .BeTrue();
        }

        [TestMethod]
        public void NaoDevePermitirVoluntarioNulo()
        {
            var contextoMock = new Mock<IContextoDados>();
            var servico = new VoluntarioService(contextoMock.Object);

            Action method = () => servico.Cadastra(null);

            method.ShouldThrow<ArgumentNullException>();
        }

        [TestMethod]
        public void NomeVoluntarioDeveSerUnico()
        {
            var contextoMock = new Mock<IContextoDados>();
            var servicoMock = new Mock<VoluntarioService>(contextoMock.Object);
            servicoMock.Setup(s => s.ExistePeloNome(It.IsAny<VoluntarioModel>()))
                .Returns(false);
            servicoMock.Protected()
                .Setup<Boolean>("ValidoCadastro", ItExpr.IsAny<VoluntarioModel>())
                .Returns(false);

            var servico = servicoMock.Object;

            var voluntario = new VoluntarioModel();
            Action method = () => servico.Cadastra(voluntario);

            method.ShouldThrow<Exception>();
        }
    }
}
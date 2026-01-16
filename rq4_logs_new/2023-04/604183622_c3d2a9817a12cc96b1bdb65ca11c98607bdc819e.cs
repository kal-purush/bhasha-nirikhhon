using AutoFixture;
using Microsoft.AspNetCore.Mvc;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tic_Tac_Toe_Web_API.Controllers;
using Tic_Tac_Toe_Web_API.Managers.Interfaces;
using Tic_Tac_Toe_Web_API.Models;
using Tic_Tac_Toe_Web_API.Models.Mappers;

namespace TicTacToeWebAPI.Tests.ControllersTests
{
    [TestFixture]
    public class RotaControllerTests
    {
        private RotaController _controller;
        private Mock<IGameManager> _gameManager;
        private RotaGameMapper _gameMapper;

        private static readonly Fixture _fixture = new Fixture();

        [SetUp]
        public void Setup()
        {
            // Arrange
            _gameManager = new Mock<IGameManager>();
            _gameMapper = new RotaGameMapper();
            _controller = new RotaController(_gameManager.Object, _gameMapper);
        }

        [Test]
        public async Task GetGameById_Should_Return_ResponseDto_With_Game_Details_If_Game_Exist()
        {
            //Arrange
            var game = _fixture.Create<RotaGame>();
            _gameManager.Setup(g => g.GetGameByIdAsync(game.Id)).ReturnsAsync(game);
            var responseDto = _gameMapper.ConvertToResponseDto(game);
            //Act
            var result = await _controller.GetGameById(game.Id);

            //Assert
            Assert.IsNotNull(result);
            Assert.IsInstanceOf<ObjectResult>(result);

            var okResult = result as ObjectResult;
            Assert.That(okResult.Value.Equals(responseDto));
        }

        [Test]
        public async Task GetGameById_Should_Return_StatusCode_200_If_Game_Exist()
        {
            //Arrange
            var game = _fixture.Create<RotaGame>();
            _gameManager.Setup(g => g.GetGameByIdAsync(game.Id)).ReturnsAsync(game);
            var responseDto = _gameMapper.ConvertToResponseDto(game);

            //Act
            var result = await _controller.GetGameById(game.Id);

            // Assert
            Assert.IsNotNull(result);
            Assert.IsInstanceOf<ObjectResult>(result);

            var okResult = result as ObjectResult;
            Assert.That(okResult.StatusCode, Is.EqualTo(200));
        }

        [Test]
        public async Task GetGameById_Should_Catch_Exception_If_GameManager_Throws_Exception()
        {
            //Arrange
            int gameId = 1;
            _gameManager.Setup(g => g.GetGameByIdAsync(gameId)).Throws<Exception>();

            //Act
            var result = await _controller.GetGameById(1);

            //Assert
            Assert.IsNotNull(result);
            Assert.IsInstanceOf<ObjectResult>(result);

            var okResult = result as ObjectResult;
            Assert.IsInstanceOf<BadRequestObjectResult>(result);
        }

        [Test]
        public async Task AddPawn_Should_Return_ResponseDto_With_Updated_Game_Details()
        {
            //Arrange
            var game = _fixture.Create<RotaGame>();
            _gameManager.Setup(g => g.GetGameByIdAsync(game.Id)).ReturnsAsync(game);
            var responseDto = _gameMapper.ConvertToResponseDto(game);

            //Act
            var result = await _controller.GetGameById(game.Id);

            //Assert
            Assert.IsNotNull(result);
            Assert.IsInstanceOf<ObjectResult>(result);

            var okResult = result as ObjectResult;
            Assert.That(okResult.Value.Equals(responseDto));
        }
    }
}
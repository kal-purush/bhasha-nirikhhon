using System;
using Microsoft.AspNetCore.Mvc;
using Moq;
using BELBBL;
using BELBModels;
using BELBRest.Controllers;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Microsoft.AspNetCore.Http;
using System.Threading.Tasks;
using BELBDL;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace BELBTests
{
    public class ControllerTest
    {
        private readonly DbContextOptions<BELBDBContext> options;
        public ControllerTest()
        {
            options = new DbContextOptionsBuilder<BELBDBContext>().UseSqlite("Filename=Test.db;foreign keys=false").Options;
        }

        [Fact]
        public async Task GetAllLeaderboardsShouldReturnList()
        {
            var mockBL = new Mock<ILeaderboardBL>();
            mockBL.Setup(x => x.GetAllLeaderboards()).ReturnsAsync(
                new List<LeaderBoard>
                {
                    new LeaderBoard(){
                        AuthId = "CM",
                        UserName = "Cesar123",
                        Name = "Cesar",
                        AverageWPM = 25,
                        AverageAcc = 5,
                        CatID = 1
                    },
                    new LeaderBoard(){
                        AuthId = "CM2",
                        UserName = "Cesar1234",
                        Name = "Cesar2",
                        AverageWPM = 30,
                        AverageAcc = 6,
                        CatID = 1
                    }
                }
                );

            var s = Options.Create(new ApiSettings());

            var controller = new LBController(mockBL.Object, s);
            var result = controller.GetAllLeaderboards();
            var okResult = await result as OkObjectResult;
            Assert.NotNull(okResult);
            Assert.True(okResult is OkObjectResult);
            Assert.IsType<List<LeaderBoard>>(okResult.Value);
            Assert.Equal(StatusCodes.Status200OK, okResult.StatusCode);
        }

        [Fact]
        public async Task GetLeaderboardByCatIDShouldReturnLeaderboardList()
        {
            var mockBL = new Mock<ILeaderboardBL>();
            mockBL.Setup(x => x.GetLeaderboardByCatId(1)).ReturnsAsync(
                new List<LeaderBoard>
                {
                    new LeaderBoard(){
                        AuthId = "CM",
                        UserName = "Cesar123",
                        Name = "Cesar",
                        AverageWPM = 25,
                        AverageAcc = 5,
                        CatID = 1
                    },
                    new LeaderBoard(){
                        AuthId = "CM2",
                        UserName = "Cesar1234",
                        Name = "Cesar2",
                        AverageWPM = 30,
                        AverageAcc = 6,
                        CatID = 1
                    }
                }
                );

            var s = Options.Create(new ApiSettings());

            var controller = new LBController(mockBL.Object, s);
            var result = controller.GetLeaderboardByCatID(1);
            var okResult = await result as OkObjectResult;
            Assert.NotNull(okResult);
            Assert.True(okResult is OkObjectResult);
            Assert.IsType<List<LeaderBoard>>(okResult.Value);
            Assert.Equal(StatusCodes.Status200OK, okResult.StatusCode);
        }

        [Fact]
        public async Task AddLeaderboardShouldReturnLeaderboard()
        {
            var mockBL = new Mock<ILeaderboardBL>();
            mockBL.Setup(x => x.AddLeaderboard(It.IsAny<LeaderBoard>())).ReturnsAsync(
                    new LeaderBoard(){
                        AuthId = "CM",
                        UserName = "Cesar123",
                        Name = "Cesar",
                        AverageWPM = 25,
                        AverageAcc = 5,
                        CatID = 1
                    }
                );


            var s = Options.Create(new ApiSettings());

            var controller = new LBController(mockBL.Object, s);
            var result = controller.AddLeaderboard(new LeaderBoard());
            var okResult = await result as OkObjectResult;
            Assert.NotNull(okResult);
            Assert.True(okResult is OkObjectResult);
            Assert.IsType<LeaderBoard>(okResult.Value);
            Assert.Equal(StatusCodes.Status200OK, okResult.StatusCode);
        }

        [Fact]
        public async Task UpdateLeaderboardShouldReturnLeaderboardList()
        {
            var mockBL = new Mock<ILeaderboardBL>();
            mockBL.Setup(x => x.Updatedleaderboard(It.IsAny<List<LeaderBoard>>())).ReturnsAsync(
                new List<LeaderBoard>
                {
                    new LeaderBoard(){
                        AuthId = "CM",
                        UserName = "Cesar123",
                        Name = "Cesar",
                        AverageWPM = 25,
                        AverageAcc = 5,
                        CatID = 1
                    },
                    new LeaderBoard(){
                        AuthId = "CM2",
                        UserName = "Cesar1234",
                        Name = "Cesar2",
                        AverageWPM = 30,
                        AverageAcc = 6,
                        CatID = 1
                    }
                }
                );

            var s = Options.Create(new ApiSettings());

            var controller = new LBController(mockBL.Object, s);
            var result = controller.UpdateLeaderboard(new List<LeaderBoard>(), 1);
            var okResult = await result as OkObjectResult;
            Assert.NotNull(okResult);
            Assert.True(okResult is OkObjectResult);
            Assert.IsType<List<LeaderBoard>>(okResult.Value);
            Assert.Equal(StatusCodes.Status200OK, okResult.StatusCode);
        }

        [Fact]
        public async Task DeleteLeaderboardShouldReturnsNoContent()
        {
            var mockBL = new Mock<ILeaderboardBL>();
            mockBL.Setup(x => x.DeleteLeaderboard(It.IsAny<string>(), It.IsAny<int>())).ReturnsAsync("ok");


            var s = Options.Create(new ApiSettings());

            var controller = new LBController(mockBL.Object, s);
            var result = controller.DeleteLeaderboard("test", 1);
            var okResult = await result as OkObjectResult;
            Assert.Null(okResult);
        }
    }
}
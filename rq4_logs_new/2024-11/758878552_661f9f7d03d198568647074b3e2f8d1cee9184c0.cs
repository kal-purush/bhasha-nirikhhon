using AutoFixture.Xunit2;
using Jared.Application.Controllers;
using Jared.Application.Requests.Tasks.Create;
using Jared.Application.Requests.Tasks.Details;
using Jared.Application.Requests.Tasks.List;
using Jared.Application.Requests.Tasks.Page;
using Jared.Application.Requests.Tasks.Update;
using Jared.Shared.Abstractions;
using Jared.Shared.Dtos.TaskDtos;
using Jared.Shared.Enums;
using MediatR;
using Moq;

namespace Jared.Application.Tests.Controllers
{
    public class TaskControllerTest
    {
        private readonly TaskController controller;
        private readonly Mock<IMediator> mediatorMock = new();

        public TaskControllerTest()
        {
            controller = new(mediatorMock.Object);
        }

        #region TaskDetailsAsync
        [Theory]
        [AutoData]
        public async Task TaskDetailsAsync_WnenMediatrReturnsOk_ShouldReturnSuccessResult(TaskDetailsDto dto)
        {
            // Arrange
            mediatorMock.Setup(x => x.Send(It.IsAny<TaskDetailsQuery>(), It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(Result.Ok(dto)));

            // Act
            var result = await controller.TaskDetailsAsync(dto.Id);

            // Assert
            Assert.True(result.Success);
            Assert.Equal(dto, result.Data);
            Assert.Equal(string.Empty, result.Error);
            mediatorMock.Verify(x => x.Send(new TaskDetailsQuery(dto.Id), default), Times.Once);
        }

        [Theory]
        [AutoData]
        public async Task TaskDetailsAsync_WnenMediatrReturnsFail_ShouldReturnFailureResult(string errorMessage)
        {
            // Arrange
            mediatorMock.Setup(x => x.Send(It.IsAny<TaskDetailsQuery>(), It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(Result.Fail<TaskDetailsDto>(errorMessage)));

            // Act
            var result = await controller.TaskDetailsAsync(1);

            // Assert
            Assert.False(result.Success);
            Assert.Null(result.Data);
            Assert.Equal(errorMessage, result.Error);
            mediatorMock.Verify(x => x.Send(new TaskDetailsQuery(1), default), Times.Once);
        }
        #endregion

        #region TaskListAsync
        [Theory]
        [AutoData]
        public async Task TaskListAsync_WnenMediatrReturnsOk_ShouldReturnSuccessResult(List<TaskListDto> dtos)
        {
            // Arrange
            mediatorMock.Setup(x => x.Send(It.IsAny<TaskListQuery>(), It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(Result.Ok(dtos)));

            // Act
            var result = await controller.TaskListAsync(null, null);

            // Assert
            Assert.True(result.Success);
            Assert.Equal(dtos, result.Data);
            Assert.Equal(string.Empty, result.Error);
            mediatorMock.Verify(x => x.Send(new TaskListQuery(null, null), default), Times.Once);
        }

        [Theory]
        [AutoData]
        public async Task TaskListAsync_WnenMediatrReturnsFail_ShouldReturnFailureResult(string errorMessage)
        {
            // Arrange
            mediatorMock.Setup(x => x.Send(It.IsAny<TaskListQuery>(), It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(Result.Fail<List<TaskListDto>>(errorMessage)));

            // Act
            var result = await controller.TaskListAsync(null, null);

            // Assert
            Assert.False(result.Success);
            Assert.Null(result.Data);
            Assert.Equal(errorMessage, result.Error);
            mediatorMock.Verify(x => x.Send(new TaskListQuery(null, null), default), Times.Once);
        }
        #endregion

        #region TaskPageAsync
        [Theory]
        [AutoData]
        public async Task TaskPageAsync_WnenMediatrReturnsOk_ShouldReturnSuccessResult(
            TaskPageDto dto,
            int page,
            int pageSize,
            string sortingProperty,
            SortingDirection sortingDirection,
            IDictionary<string, string?>? filter)
        {
            // Arrange
            mediatorMock.Setup(x => x.Send(It.IsAny<TaskPageQuery>(), It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(Result.Ok(dto)));

            // Act
            var result = await controller.TaskPageAsync(page, pageSize, sortingProperty, sortingDirection, filter);

            // Assert
            Assert.True(result.Success);
            Assert.Equal(dto, result.Data);
            Assert.Equal(string.Empty, result.Error);
            mediatorMock.Verify(x => x.Send(
                new TaskPageQuery(page, pageSize, sortingProperty, sortingDirection, filter), default), Times.Once);
        }

        [Theory]
        [AutoData]
        public async Task TaskPageAsync_WnenMediatrReturnsFail_ShouldReturnFailureResult(
            string errorMessage,
            int page,
            int pageSize,
            string sortingProperty,
            SortingDirection sortingDirection,
            IDictionary<string, string?>? filter)
        {
            // Arrange
            mediatorMock.Setup(x => x.Send(It.IsAny<TaskPageQuery>(), It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(Result.Fail<TaskPageDto>(errorMessage)));

            // Act
            var result = await controller.TaskPageAsync(page, pageSize, sortingProperty, sortingDirection, filter);

            // Assert
            Assert.False(result.Success);
            Assert.Null(result.Data);
            Assert.Equal(errorMessage, result.Error);
            mediatorMock.Verify(x => x.Send(
                new TaskPageQuery(page, pageSize, sortingProperty, sortingDirection, filter), default), Times.Once);
        }
        #endregion

        #region TaskUpdateAsync
        [Theory]
        [AutoData]
        public async Task TaskUpdateAsync_WnenMediatrReturnsOk_ShouldReturnSuccessResult(TaskDetailsDto dto)
        {
            // Arrange
            mediatorMock.Setup(x => x.Send(It.IsAny<TaskUpdateCommand>(), It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(Result.Ok(true)));

            // Act
            var result = await controller.TaskUpdateAsync(dto);

            // Assert
            Assert.True(result.Success);
            Assert.True(result.Data);
            Assert.Equal(string.Empty, result.Error);
            mediatorMock.Verify(x => x.Send(new TaskUpdateCommand(dto), default), Times.Once);
        }

        [Theory]
        [AutoData]
        public async Task TaskUpdateAsync_WnenMediatrReturnsFail_ShouldReturnFailureResult(
            TaskDetailsDto dto,
            string errorMessage)
        {
            // Arrange
            mediatorMock.Setup(x => x.Send(It.IsAny<TaskUpdateCommand>(), It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(Result.Fail<bool>(errorMessage)));

            // Act
            var result = await controller.TaskUpdateAsync(dto);

            // Assert
            Assert.False(result.Success);
            Assert.False(result.Data);
            Assert.Equal(errorMessage, result.Error);
            mediatorMock.Verify(x => x.Send(new TaskUpdateCommand(dto), default), Times.Once);
        }
        #endregion

        #region TaskCreateAsync
        [Theory]
        [AutoData]
        public async Task TaskCreateAsync_WnenMediatrReturnsOk_ShouldReturnSuccessResult(TaskDetailsDto dto)
        {
            // Arrange
            mediatorMock.Setup(x => x.Send(It.IsAny<TaskCreateCommand>(), It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(Result.Ok(true)));

            // Act
            var result = await controller.TaskCreateAsync(dto);

            // Assert
            Assert.True(result.Success);
            Assert.True(result.Data);
            Assert.Equal(string.Empty, result.Error);
            mediatorMock.Verify(x => x.Send(new TaskCreateCommand(dto), default), Times.Once);
        }

        [Theory]
        [AutoData]
        public async Task TaskCreateAsync_WnenMediatrReturnsFail_ShouldReturnFailureResult(
            TaskDetailsDto dto,
            string errorMessage)
        {
            // Arrange
            mediatorMock.Setup(x => x.Send(It.IsAny<TaskCreateCommand>(), It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(Result.Fail<bool>(errorMessage)));

            // Act
            var result = await controller.TaskCreateAsync(dto);

            // Assert
            Assert.False(result.Success);
            Assert.False(result.Data);
            Assert.Equal(errorMessage, result.Error);
            mediatorMock.Verify(x => x.Send(new TaskCreateCommand(dto), default), Times.Once);
        }
        #endregion
    }
}
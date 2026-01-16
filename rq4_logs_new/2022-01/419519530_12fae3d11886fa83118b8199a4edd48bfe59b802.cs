using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using LT.DigitalOffice.Kernel.BrokerSupport.AccessValidatorEngine.Interfaces;
using LT.DigitalOffice.Kernel.Constants;
using LT.DigitalOffice.Kernel.Enums;
using LT.DigitalOffice.Kernel.Helpers.Interfaces;
using LT.DigitalOffice.Kernel.Responses;
using LT.DigitalOffice.PositionService.Business.Commands.Position;
using LT.DigitalOffice.PositionService.Business.Commands.Position.Interfaces;
using LT.DigitalOffice.PositionService.Data.Interfaces;
using LT.DigitalOffice.PositionService.Mappers.Db.Interfaces;
using LT.DigitalOffice.PositionService.Models.Db;
using LT.DigitalOffice.PositionService.Models.Dto.Requests.Position;
using LT.DigitalOffice.PositionService.Validation.Position.Interfaces;
using LT.DigitalOffice.UnitTestKernel;
using Microsoft.AspNetCore.Http;
using Moq;
using Moq.AutoMock;
using NUnit.Framework;

namespace PositionService.Business.UnitTests
{
  class CreatePositionCommandTests
  {
    private AutoMocker _autoMocker;
    private ICreatePositionCommand _command;

    private CreatePositionRequest _request;
    private DbPosition _dbPosition;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
      _autoMocker = new AutoMocker();
      _command = _autoMocker.CreateInstance<CreatePositionCommand>();

      _request = new CreatePositionRequest()
      {
        Name = "Name",
        Description = "Description"
      };

      _dbPosition = new DbPosition()
      {
        Id = Guid.NewGuid(),
        Name = _request.Name,
        Description = _request.Description,
        IsActive = true,
        CreatedAtUtc = DateTime.UtcNow,
        CreatedBy = Guid.NewGuid()
      };

      _autoMocker
          .Setup<IHttpContextAccessor, int>(a => a.HttpContext.Response.StatusCode)
          .Returns(200);

      _autoMocker
        .Setup<IResponseCreator, OperationResultResponse<Guid?>>(x => x.CreateFailureResponse<Guid?>(HttpStatusCode.BadRequest, It.IsAny<List<string>>()))
        .Returns(new OperationResultResponse<Guid?>()
        {
          Status = OperationResultStatusType.Failed,
          Errors = new() { "Request is not correct." }
        });

      _autoMocker
        .Setup<IResponseCreator, OperationResultResponse<Guid?>>(x => x.CreateFailureResponse<Guid?>(HttpStatusCode.Forbidden, It.IsAny<List<string>>()))
        .Returns(new OperationResultResponse<Guid?>()
        {
          Status = OperationResultStatusType.Failed,
          Errors = new() { "Not enough rights." }
        });
    }

    [SetUp]
    public void Setup()
    {
      _autoMocker.GetMock<IAccessValidator>().Reset();
      _autoMocker.GetMock<ICreatePositionRequestValidator>().Reset();
      _autoMocker.GetMock<IDbPositionMapper>().Reset();
      _autoMocker.GetMock<IPositionRepository>().Reset();

      _autoMocker
        .Setup<IAccessValidator, Task<bool>>(x => x.HasRightsAsync(Rights.AddEditRemovePositions))
        .ReturnsAsync(true);

      _autoMocker
        .Setup<ICreatePositionRequestValidator, bool>(x => x.ValidateAsync(_request, default).Result.IsValid)
        .Returns(true);

      _autoMocker
        .Setup<IDbPositionMapper, DbPosition>(x => x.Map(_request))
        .Returns(_dbPosition);

      _autoMocker
        .Setup<IPositionRepository, Task<Guid?>>(x => x.CreateAsync(_dbPosition))
        .ReturnsAsync(_dbPosition.Id);
    }

    [Test]
    public async Task ShouldReturnFailedResponseWhenUserHasNotRightAsync()
    {
      _autoMocker
        .Setup<IAccessValidator, Task<bool>>(x => x.HasRightsAsync(Rights.AddEditRemovePositions))
        .ReturnsAsync(false);

      OperationResultResponse<Guid?> expectedResponse = new()
      {
        Status = OperationResultStatusType.Failed,
        Errors = new List<string> { "Not enough rights." }
      };

      SerializerAssert.AreEqual(expectedResponse, await _command.ExecuteAsync(_request));

      _autoMocker.Verify<IAccessValidator>(
        x => x.HasRightsAsync(It.IsAny<int>()),
        Times.Once);

      _autoMocker.Verify<ICreatePositionRequestValidator>(
        x => x.ValidateAsync(It.IsAny<CreatePositionRequest>(), It.IsAny<CancellationToken>()),
        Times.Never);

      _autoMocker.Verify<IDbPositionMapper>(
        x => x.Map(It.IsAny<CreatePositionRequest>()),
        Times.Never);

      _autoMocker.Verify<IPositionRepository>(
        x => x.CreateAsync(It.IsAny<DbPosition>()),
        Times.Never);
    }

    [Test]
    public async Task ShouldReturnFailedResponseWhenValidationIsFailedAsync()
    {
      _autoMocker
        .Setup<ICreatePositionRequestValidator, bool>(x => x.ValidateAsync(_request, default).Result.IsValid)
        .Returns(false);

      OperationResultResponse<Guid?> expectedResponse = new()
      {
        Status = OperationResultStatusType.Failed,
        Errors = new List<string> { "Request is not correct." }
      };

      SerializerAssert.AreEqual(expectedResponse, await _command.ExecuteAsync(_request));

      _autoMocker.Verify<IAccessValidator>(
        x => x.HasRightsAsync(It.IsAny<int>()),
        Times.Once);

      _autoMocker.Verify<ICreatePositionRequestValidator>(
        x => x.ValidateAsync(It.IsAny<CreatePositionRequest>(), It.IsAny<CancellationToken>()),
        Times.Once);

      _autoMocker.Verify<IDbPositionMapper>(
        x => x.Map(It.IsAny<CreatePositionRequest>()),
        Times.Never);

      _autoMocker.Verify<IPositionRepository>(
        x => x.CreateAsync(It.IsAny<DbPosition>()),
        Times.Never);
    }

    [Test]
    public async Task ShouldReturnFailedResponseWhenRepositoryReturnNullAsync()
    {
      _autoMocker
        .Setup<IPositionRepository, Task<Guid?>>(x => x.CreateAsync(_dbPosition))
        .ReturnsAsync((Guid?)null);

      OperationResultResponse<Guid?> expectedResponse = new()
      {
        Status = OperationResultStatusType.Failed,
        Errors = new List<string> { "Request is not correct." }
      };

      SerializerAssert.AreEqual(expectedResponse, await _command.ExecuteAsync(_request));

      _autoMocker.Verify<IAccessValidator>(
        x => x.HasRightsAsync(It.IsAny<int>()),
        Times.Once);

      _autoMocker.Verify<ICreatePositionRequestValidator>(
        x => x.ValidateAsync(It.IsAny<CreatePositionRequest>(), It.IsAny<CancellationToken>()),
        Times.Once);

      _autoMocker.Verify<IDbPositionMapper>(
        x => x.Map(It.IsAny<CreatePositionRequest>()),
        Times.Once);

      _autoMocker.Verify<IPositionRepository>(
        x => x.CreateAsync(It.IsAny<DbPosition>()),
        Times.Once);
    }

    [Test]
    public async Task ShouldCreatePositionSuccesfullAsync()
    {
      OperationResultResponse<Guid?> expectedResponse = new()
      {
        Status = OperationResultStatusType.FullSuccess,
        Body = _dbPosition.Id
      };

      SerializerAssert.AreEqual(expectedResponse, await _command.ExecuteAsync(_request));

      _autoMocker.Verify<IAccessValidator>(
        x => x.HasRightsAsync(It.IsAny<int>()),
        Times.Once);

      _autoMocker.Verify<ICreatePositionRequestValidator>(
        x => x.ValidateAsync(It.IsAny<CreatePositionRequest>(), It.IsAny<CancellationToken>()),
        Times.Once);

      _autoMocker.Verify<IDbPositionMapper>(
        x => x.Map(It.IsAny<CreatePositionRequest>()),
        Times.Once);

      _autoMocker.Verify<IPositionRepository>(
        x => x.CreateAsync(It.IsAny<DbPosition>()),
        Times.Once);
    }
  }
}
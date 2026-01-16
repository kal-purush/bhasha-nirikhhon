using LT.DigitalOffice.DashboardService.Business.TaskType.Interfaces;
using LT.DigitalOffice.DashboardService.Data.Interfaces;
using LT.DigitalOffice.DashboardService.Models.Dto.Requests.TaskType;
using LT.DigitalOffice.Kernel.Responses;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Threading.Tasks;

namespace LT.DigitalOffice.DashboardService.Business.TaskType;

public class CreateTaskTypeCommand : ICreateTaskTypeCommand
{
  private readonly ITaskTypeRepository _repository;

  public CreateTaskTypeCommand(ITaskTypeRepository repository)
  {
    _repository = repository;
  }

  public Task<OperationResultResponse<Guid?>> ExecuteAsync(CreateTaskTypeRequest request)
  {
    throw new NotImplementedException();
  }
}
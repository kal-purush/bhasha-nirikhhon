using LT.DigitalOffice.DashboardService.Business.Task.Interfaces;
using LT.DigitalOffice.DashboardService.Data.Interfaces;
using LT.DigitalOffice.DashboardService.Models.Dto.Requests.Task;
using LT.DigitalOffice.Kernel.Responses;
using System;
using System.Threading.Tasks;

namespace LT.DigitalOffice.DashboardService.Business.Task;

public class CreateTaskCommand : ICreateTaskCommand
{
  private readonly ITaskRepository _taskRepository;

  public CreateTaskCommand(ITaskRepository taskRepository)
  {
    _taskRepository = taskRepository;
  }
  
  public Task<OperationResultResponse<Guid?>> ExecuteAsync(CreateTaskRequest request)
  {
    throw new NotImplementedException();
  }
}
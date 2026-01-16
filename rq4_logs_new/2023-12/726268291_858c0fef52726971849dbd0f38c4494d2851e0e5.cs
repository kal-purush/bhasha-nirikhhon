using LT.DigitalOffice.DashboardService.Business.Group.Interfaces;
using LT.DigitalOffice.DashboardService.Data.Interfaces;
using LT.DigitalOffice.DashboardService.Models.Dto.Requests.Group;
using LT.DigitalOffice.Kernel.Responses;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Threading.Tasks;

namespace LT.DigitalOffice.DashboardService.Business.Group;

public class CreateGroupCommand : ICreateGroupCommand
{
  private readonly IGroupRepository _repository;

  public CreateGroupCommand(IGroupRepository repository)
  {
    _repository = repository;
  }

  public Task<OperationResultResponse<Guid?>> ExecuteAsync(CreateGroupRequest request)
  {
    throw new NotImplementedException();
  }
}
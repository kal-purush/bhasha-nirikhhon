using LT.DigitalOffice.DashboardService.Business.Comment.Interfaces;
using LT.DigitalOffice.DashboardService.Data.Interfaces;
using LT.DigitalOffice.DashboardService.Models.Dto.Requests.Comment;
using LT.DigitalOffice.Kernel.Responses;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Threading.Tasks;

namespace LT.DigitalOffice.DashboardService.Business.Comment;

public class CreateCommentCommand : ICreateCommentCommand
{
  private readonly ICommentRepository _repository;

  public CreateCommentCommand(ICommentRepository repository)
  {
    _repository = repository;
  }

  public Task<OperationResultResponse<Guid?>> ExecuteAsync(CreateCommentRequest request)
  {
    throw new NotImplementedException();
  }
}
using LT.DigitalOffice.DashboardService.Business.Board.Interfaces;
using LT.DigitalOffice.DashboardService.Models.Dto.Requests.Board;
using LT.DigitalOffice.Kernel.Responses;
using System.Threading.Tasks;
using System;
using LT.DigitalOffice.DashboardService.Data.Interfaces;
using LT.DigitalOffice.DashboardService.Mappers.Db.Interfaces;
using Microsoft.AspNetCore.Http;
using LT.DigitalOffice.Kernel.BrokerSupport.AccessValidatorEngine.Interfaces;
using LT.DigitalOffice.Kernel.Helpers.Interfaces;

namespace LT.DigitalOffice.DashboardService.Business.Board;

public class CreateBoardCommand : ICreateBoardCommand
{
  private readonly IBoardRepository _boardRepository;
  private readonly IDbBoardMapper _dbBoardMapper;
  private readonly IHttpContextAccessor _httpContextAccessor;
  private readonly IAccessValidator _accessValidator;
  private readonly IResponseCreator _responseCreator;

  public CreateBoardCommand(
    IBoardRepository repository,
    IDbBoardMapper dbBoardMapper,
    IHttpContextAccessor httpContext,
    IAccessValidator accessValidator,
    IResponseCreator responseCreator)
  {
    _boardRepository = repository;
    _dbBoardMapper = dbBoardMapper;
    _httpContextAccessor = httpContext;
    _accessValidator = accessValidator;
    _responseCreator = responseCreator;
  }

  public Task<OperationResultResponse<Guid?>> ExecuteAsync(CreateBoardRequest request)
  {
    throw new NotImplementedException();
  }
}

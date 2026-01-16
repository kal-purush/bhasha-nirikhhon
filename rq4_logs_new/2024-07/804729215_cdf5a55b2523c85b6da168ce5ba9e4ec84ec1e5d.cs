using Application.Abstractions.Data;
using Contract.Abstractions.Messages;
using Contract.Abstractions.Shared.Results;
using Contract.Abstractions.Shared.Search;
using Contract.Services.SalaryHistory.Queries;
using Contract.Services.SalaryHistory.ShareDtos;
using Domain.Exceptions.Users;

namespace Application.UserCases.Queries.SalaryHistories;

public sealed class GetSalaryByDayByUserIdQueryHandler
    (ISalaryHistoryRepository _salaryHistoryRepository,
    IUserRepository _userRepository
    ) : IQueryHandler<GetSalaryByDayByUserIdQuery, SearchResponse<List<SalaryByDayResponse>>>
{
    public async Task<Result.Success<SearchResponse<List<SalaryByDayResponse>>>> Handle(GetSalaryByDayByUserIdQuery request, CancellationToken cancellationToken)
    {
        if (request.RoleName != "MAIN_ADMIN" && request.UserIdClaims != request.UserId)
        {
            throw new UserNotPermissionException("Bạn không có quyền xem thông tin lương nhân viên khác.");
        }
        var isExistUser = await _userRepository.IsUserActiveAsync(request.UserId);
        if (!isExistUser)
        {
            throw new UserNotFoundException(request.UserId);
        }
        var query = await _salaryHistoryRepository.GetSalaryHistoryByUserId(request.UserId, SalaryType.SALARY_BY_DAY, request.PageIndex, request.PageSize);
        var salaryByDayResponses = query.Item1;
        var totalPage = query.Item2;

        var data = salaryByDayResponses.ConvertAll(s => new SalaryByDayResponse(s.Salary, s.StartDate));
        var searchResponse = new SearchResponse<List<SalaryByDayResponse>>(request.PageIndex, totalPage, data);
        return Result.Success<SearchResponse<List<SalaryByDayResponse>>>.Get(searchResponse);

    }
}
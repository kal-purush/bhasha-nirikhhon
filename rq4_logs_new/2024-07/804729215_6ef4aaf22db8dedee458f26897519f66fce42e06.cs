using Application.Abstractions.Data;
using Application.Abstractions.Services;
using Contract.Abstractions.Messages;
using Contract.Abstractions.Shared.Results;
using Contract.Abstractions.Shared.Search;
using Contract.Services.MonthEmployeeSalary.Queries;
using Contract.Services.MonthEmployeeSalary.ShareDtos;

namespace Application.UserCases.Queries.MonthlyEmployeeSalaries;

public sealed class GetMonthlySalaryQueryHandler
    (IMonthlyEmployeeSalaryRepository _monthlyEmployeeSalaryRepository,
    ICloudStorage _cloudStorage
    ) : IQueryHandler<GetMonthlySalaryQuery, SearchResponse<List<MonthlySalaryResponse>>>
{
    public async Task<Result.Success<SearchResponse<List<MonthlySalaryResponse>>>> Handle(GetMonthlySalaryQuery request, CancellationToken cancellationToken)
    {
        var query = await _monthlyEmployeeSalaryRepository.SearchMonthlySalary(request);
        var totalRecords = query.Item2;
        var monthlyEmployeeSalaries = query.Item1;
        var monthlySalaryResponses = new List<MonthlySalaryResponse>();

        foreach (var monthlyEmployeeSalary in monthlyEmployeeSalaries)
        {
            var monthlySalaryResponse = new MonthlySalaryResponse
            (
                Id: monthlyEmployeeSalary.Id,
                UserId: monthlyEmployeeSalary.User.Id,
                FullName: monthlyEmployeeSalary.User.FirstName + " " + monthlyEmployeeSalary.User.LastName,
                Avatar: await _cloudStorage.GetSignedUrlAsync(monthlyEmployeeSalary.User.Avatar),
                Month: monthlyEmployeeSalary.Month,
                Year: monthlyEmployeeSalary.Year,
                Salary: monthlyEmployeeSalary.Salary
            );
            monthlySalaryResponses.Add(monthlySalaryResponse);
        }
        var searchReponse = new SearchResponse<List<MonthlySalaryResponse>>(request.PageIndex, totalRecords, monthlySalaryResponses);
        return Result.Success<SearchResponse<List<MonthlySalaryResponse>>>.Get(searchReponse);

    }
}

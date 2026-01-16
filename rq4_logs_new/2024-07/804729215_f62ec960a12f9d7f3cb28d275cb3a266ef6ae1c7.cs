using Application.Abstractions.Data;
using Application.Abstractions.Services;
using Contract.Abstractions.Messages;
using Contract.Abstractions.Shared.Results;
using Contract.Services.MonthEmployeeSalary.Queries;
using Contract.Services.MonthEmployeeSalary.ShareDtos;
using Domain.Exceptions.Users;
using System.Linq;

namespace Application.UserCases.Queries.MonthlyEmployeeSalaries;

public sealed class GetMonthlyEmployeeSalaryByUserIdQueryHandler
    (IMonthlyEmployeeSalaryRepository _monthlyEmployeeSalaryRepository,
    IEmployeeProductRepository _employeeProductRepository,
    IAttendanceRepository _attendanceRepository,
    ICloudStorage _cloudStorage
    ) : IQueryHandler<GetMonthlyEmployeeSalaryByUserIdQuery, MonthlyEmployeeSalaryResponse>
{
    public async Task<Result.Success<MonthlyEmployeeSalaryResponse>> Handle(GetMonthlyEmployeeSalaryByUserIdQuery request, CancellationToken cancellationToken)
    {
        if (request.RoleNameClaim != "MAIN_ADMIN" && request.UserId != request.UserId)
        {
            throw new UserNotPermissionException("Bạn không có quyền xem thông tin lương của nhân viên khác!");
        }

        var monthlyEmployeeSalary = await _monthlyEmployeeSalaryRepository
            .GetMonthlyEmployeeSalaryByUserIdAsync(request.UserId, request.Month, request.Year);

        var employeeProductDetails = await _employeeProductRepository
            .GetEmployeeProductsByMonthAndYearAndUserId(request.Month, request.Year, request.UserId);

        var productWorkingResponses = employeeProductDetails.Select(async ep =>
        {
            var productName = ep.Product.Name;
            var quantity = ep.Quantity;

            // Find salary per product based on phase
            var salaryPerProduct = ep.Product.ProductPhaseSalaries
                .Where(pps => pps.PhaseId == ep.PhaseId)
                .FirstOrDefault()?.SalaryPerProduct;

            // Calculate total salary
            var totalSalary = quantity * salaryPerProduct;

            // Create the response object
            return new ProductWorkingResponse(
                ProductId: ep.ProductId,
                ProductName: productName,
                ProductImage: await _cloudStorage
                                .GetSignedUrlAsync(ep.Product.Images
                                                    .FirstOrDefault()
                                                    ?.IsMainImage ?? false 
                                                    ? ep.Product.Images.FirstOrDefault()
                                                    ?.ImageUrl : "ImageNotFound"),
                PhaseId: ep.PhaseId,
                PhaseName: ep.Phase.Name,
                PhaseDescription: ep.Phase.Description,
                Quantity: quantity,
                SalaryPerProduct: salaryPerProduct ?? 0
            );
        });

        var attendanceDetails = await _attendanceRepository
            .GetAttendanceByMonthAndUserIdAsync(request.Month, request.Year, request.UserId);

        var totalWorkingDays = (double)attendanceDetails.Count(a => a.IsAttendance && !a.IsSalaryByProduct && (a.SlotId == 1 || a.SlotId ==2))/2;
        var totalHourOverTime = attendanceDetails.Where(a => a.IsAttendance && !a.IsSalaryByProduct && a.HourOverTime > 0).Sum(a=>a.HourOverTime);
        var totalSalaryProduct = productWorkingResponses.Sum(p => p.Result.Quantity * p.Result.SalaryPerProduct);
        
        var response = new MonthlyEmployeeSalaryResponse(
            Month: request.Month,
            Year: request.Year,
            AccountBalance: monthlyEmployeeSalary.User.AccountBalance ?? 0,
            TotalWorkingDays: totalWorkingDays,
            TotalWorkingHours: totalHourOverTime,
            TotalSalaryProduct: totalSalaryProduct,
            ProductWorkingResponses: productWorkingResponses.Select(p => p.Result).ToList()
            );

        return Result.Success<MonthlyEmployeeSalaryResponse>.Get(response);
    }
}
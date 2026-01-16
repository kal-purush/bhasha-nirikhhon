using Common.Configurations;
using Common.Dtos;
using EMS.Application.DTOs.EM;
using EMS.Application.Services.EM;
using EMS.Domain.Filters.EMS;
using Microsoft.AspNetCore.Mvc;

namespace EMS.API.Endpoints.EM
{
    public static class SalaryRecordEndpoints
    {
        public static void Map(WebApplication app)
        {
            var salaryHistoryGroup = app.MapGroup("/salary-record")
                .WithTags("SalaryRecord");

            #region Get Salary Records By Filter
            salaryHistoryGroup.MapGet("/", async (ISalaryRecordService salaryRecordService, [AsParameters] SalaryRecordFilter filter) =>
            {
                try
                {
                    filter ??= new SalaryRecordFilter();
                    var pagedSalaryRecords = await salaryRecordService.GetPagedSalaryHistoriesAsync(filter);
                    var response = BaseResponse<PagedDto<SalaryRecordResponseDto>>.Success(pagedSalaryRecords);
                    return Results.Ok(response);
                }
                catch (Exception ex)
                {
                    var errorResponse = BaseResponse<PagedDto<SalaryRecordResponseDto>>.Failure("An error occurred while retrieving leave requests.");
                    return Results.Problem(detail: errorResponse.Errors[0], statusCode: errorResponse.StatusCode);
                }
            }).ConfigureApiResponses();
            #endregion

            #region Get Salary Record By Id
            salaryHistoryGroup.MapGet("/{id}", async (ISalaryRecordService salaryRecordService, long id) =>
            {
                try
                {
                    var salaryRecord = await salaryRecordService.GetSalaryHistoryByIdAsync(id);
                    if (salaryRecord == null)
                    {
                        var errorResponse = BaseResponse<SalaryRecordResponseDto>.Failure("Salary history not found.");
                        return Results.NotFound(errorResponse);
                    }
                    return Results.Ok(BaseResponse<SalaryRecordResponseDto>.Success(salaryRecord));
                }
                catch (Exception ex)
                {
                    var errorResponse = BaseResponse<IEnumerable<SalaryRecordResponseDto>>.Failure("An error occurred while retrieving salary history.");
                    return Results.Problem(detail: errorResponse.Errors[0], statusCode: errorResponse.StatusCode);
                }
            }).ConfigureApiResponses();
            #endregion

            #region Add New Salary Record
            salaryHistoryGroup.MapPost("/", async (ISalaryRecordService salaryHistoryService, [FromBody] SalaryRecordRequestDto newSalaryDto) =>
            {
                if (newSalaryDto == null)
                {
                    var errorResponse = BaseResponse<SalaryRecordResponseDto>.Failure("Salary data is required.");
                    return Results.BadRequest(errorResponse);
                }

                try
                {
                    var salaryHistoryEntry = await salaryHistoryService.CreateSalaryHistoryAsync(newSalaryDto);
                    return Results.Ok(BaseResponse<SalaryRecordResponseDto>.Success(salaryHistoryEntry));
                }
                catch (ArgumentException ex)
                {
                    var errorResponse = BaseResponse<SalaryRecordResponseDto>.Failure(ex.Message);
                    return Results.BadRequest(errorResponse);
                }
                catch (Exception ex)
                {
                    var errorResponse = BaseResponse<SalaryRecordResponseDto>.Failure("An error occurred while adding the salary record.");
                    return Results.Problem(detail: errorResponse.Errors[0], statusCode: errorResponse.StatusCode);
                }
            }).ConfigureApiResponses();
            #endregion

            #region Delete Salary Record By Id
            salaryHistoryGroup.MapDelete("/{id}", async (ISalaryRecordService salaryRecordService, long id) =>
            {
                try
                {
                    var isDeleted = await salaryRecordService.DeleteSalaryHistoryAsync(id);
                    if (!isDeleted)
                    {
                        var errorResponse = BaseResponse<object>.Failure("Salary history not found or unable to delete.");
                        return Results.NotFound(errorResponse);
                    }

                    return Results.Ok(BaseResponse<object>.Success("Salary record deleted successfully."));
                }
                catch (Exception ex)
                {
                    var errorResponse = BaseResponse<object>.Failure("An error occurred while deleting the salary history.");
                    return Results.Problem(detail: errorResponse.Errors[0], statusCode: errorResponse.StatusCode);
                }
            }).ConfigureApiResponses();
            #endregion

        }
    }
}
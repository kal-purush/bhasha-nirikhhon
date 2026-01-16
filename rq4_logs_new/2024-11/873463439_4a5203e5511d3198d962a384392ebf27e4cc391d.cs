using Common.Dtos;
using EMS.Application.DTOs.EM;
using EMS.Domain.Filters.EMS;

namespace EMS.Application.Services.EM
{
    public interface ISalaryRecordService
    {
        Task<SalaryRecordResponseDto> GetSalaryHistoryByIdAsync(string id);
        Task<SalaryRecordResponseDto> CreateSalaryHistoryAsync(SalaryRecordRequestDto salaryHistoryRequestDto);
        Task<SalaryRecordResponseDto> UpdateSalaryHistoryAsync(string id, SalaryRecordRequestDto salaryHistoryRequestDto);
        Task<bool> DeleteSalaryHistoryAsync(string id);
        Task<PagedDto<SalaryRecordResponseDto>> GetPagedSalaryHistoriesAsync(SalaryRecordFilter filter);
    }
}
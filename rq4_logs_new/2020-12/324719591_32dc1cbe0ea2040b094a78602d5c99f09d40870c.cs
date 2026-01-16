using System.Threading.Tasks;
using HaloBiz.DTOs.ApiDTOs;
using HaloBiz.DTOs.ReceivingDTOs;

namespace HaloBiz.MyServices
{
    public interface IOfficeService
    {
        Task<ApiResponse> AddOffice(OfficeReceivingDTO officeReceivingDTO);
        Task<ApiResponse> GetAllOffices();
        Task<ApiResponse> GetOfficeById(long id);
        Task<ApiResponse> GetOfficeByName(string name);
        Task<ApiResponse> UpdateOffice(long id, OfficeReceivingDTO branchReceivingDTO);
        Task<ApiResponse> DeleteOffice(long id);

    }
}
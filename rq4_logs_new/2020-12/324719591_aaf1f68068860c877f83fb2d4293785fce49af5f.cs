using System.Threading.Tasks;
using HaloBiz.DTOs.ApiDTOs;
using HaloBiz.DTOs.ReceivingDTOs;

namespace HaloBiz.MyServices
{
    public interface IServiceGroupService
    {
        Task<ApiResponse> AddServiceGroup(ServiceGroupReceivingDTO serviceGroupReceivingDTO);
        Task<ApiResponse> GetAllServiceGroups();
        Task<ApiResponse> GetServiceGroupById(long id);
        Task<ApiResponse> GetServiceGroupByName(string name);
        Task<ApiResponse> UpdateServiceGroup(long id, ServiceGroupReceivingDTO serviceGroupReceivingDTO);
        Task<ApiResponse> DeleteServiceGroup(long id);
    }
}
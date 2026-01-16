using System.Threading.Tasks;
using HaloBiz.DTOs.ApiDTOs;
using HaloBiz.DTOs.ReceivingDTOs;

namespace HaloBiz.MyServices
{
    public interface IServicesService
    {
        Task<ApiResponse> AddService(ServicesReceivingDTO servicesReceivingDTO);
        Task<ApiResponse> GetAllServices();
        Task<ApiResponse> GetServiceById(long id);
        Task<ApiResponse> GetServiceByName(string name);
        Task<ApiResponse> UpdateServices(long id, ServicesReceivingDTO serviceReceivingDTO);
        Task<ApiResponse> DeleteServiceGroup(long id);
    }
}
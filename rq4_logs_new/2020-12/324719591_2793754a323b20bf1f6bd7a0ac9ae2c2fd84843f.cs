using System.Threading.Tasks;
using HaloBiz.DTOs.ApiDTOs;
using HaloBiz.DTOs.ReceivingDTOs;

namespace HaloBiz.MyServices
{
    public interface IServiceCategoryService
    {
        Task<ApiResponse> AddServiceCategory(ServiceCategoryReceivingDTO serviceCategoryReceivingDTO);
        Task<ApiResponse> GetAllServiceCategory();
        Task<ApiResponse> GetServiceCategoryById(long id);
        Task<ApiResponse> GetServiceCategoryByName(string name);
        Task<ApiResponse> UpdateServiceCategory(long id, ServiceCategoryReceivingDTO serviceCategoryReceivingDTO);
        Task<ApiResponse> DeleteServiceCategory(long id);
        
    }
}
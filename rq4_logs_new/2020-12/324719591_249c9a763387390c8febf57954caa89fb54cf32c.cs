using HaloBiz.DTOs.ApiDTOs;
using HaloBiz.DTOs.ReceivingDTOs;
using HaloBiz.MyServices;
using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;

namespace HaloBiz.Controllers
{
    [Route("api/v1/[controller]")]
    [ApiController]
    public class ServiceCategoryController : ControllerBase
    {
        
        private readonly IServiceCategoryService _serviceCategoryService ;

        public ServiceCategoryController(IServiceCategoryService serviceCategoryService)
        {
            this._serviceCategoryService = serviceCategoryService;
        }

        [HttpGet("")]
        public async Task<ActionResult> GetServiceCategories()
        {
            var response = await _serviceCategoryService.GetAllServiceCategory();
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var serviceCategories = ((ApiOkResponse)response).Result;
            return Ok(serviceCategories);
        }

        [HttpGet("name/{name}")]
        public async Task<ActionResult> GetByName(string name)
        {
            var response = await _serviceCategoryService.GetServiceCategoryByName(name);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var serviceCategory = ((ApiOkResponse)response).Result;
            return Ok(serviceCategory);
        }

        [HttpGet("{id}")]
        public async Task<ActionResult> GetById(long id)
        {
            var response = await _serviceCategoryService.GetServiceCategoryById(id);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var serviceCategory = ((ApiOkResponse)response).Result;
            return Ok(serviceCategory);
        }

        [HttpPost("")]
        public async Task<ActionResult> AddNew(ServiceCategoryReceivingDTO serviceCategoryReceivingDTO)
        {
            var response = await _serviceCategoryService.AddServiceCategory(serviceCategoryReceivingDTO);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var serviceCategory = ((ApiOkResponse)response).Result;
            return Ok(serviceCategory);
        }

        [HttpPut("{id}")]
        public async Task<IActionResult> UpdateById(long id, ServiceCategoryReceivingDTO serviceCategoryReceivingDTO)
        {
            var response = await _serviceCategoryService.UpdateServiceCategory(id, serviceCategoryReceivingDTO);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var serviceCategory = ((ApiOkResponse)response).Result;
            return Ok(serviceCategory);
        }

        [HttpDelete("{id}")]
        public async Task<ActionResult> DeleteById(int id)
        {
            var response = await _serviceCategoryService.DeleteServiceCategory(id);
            return StatusCode(response.StatusCode);
        }
    }
}
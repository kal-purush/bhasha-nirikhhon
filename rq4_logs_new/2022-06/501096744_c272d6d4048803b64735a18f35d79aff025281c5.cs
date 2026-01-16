using AutoMapper;
using FoodZone.Services.IServices;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FoodZone.API.AdminController
{
    [Route("api/[controller]")]
    [ApiController]
    public class MenuManagementController : ControllerBase
    {
        private readonly IMenuServices _menuServices;
        private readonly IMapper _mapper;

        public MenuManagementController(IMenuServices menuServices, IMapper mapper)
        {
            _menuServices = menuServices;
            _mapper = mapper;
        }

        [HttpGet]
        [Route("getall")]
        public async Task<IActionResult> GetAll()
        {
            var menu = await _menuServices.GetAllAsync();
            return Ok(menu);
        }
    }
}
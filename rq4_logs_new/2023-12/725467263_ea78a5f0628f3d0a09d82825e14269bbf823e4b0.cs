using Microsoft.AspNetCore.Mvc;
using toiec_web.Services.IService;
using toiec_web.ViewModels.VipPackage;

namespace toiec_web.Controllers
{
    public class VipPackageController : BaseAPIController
    {
        private readonly IVipPackageService _vipPackageService;

        public VipPackageController(IVipPackageService vipPackageService) 
        {
            _vipPackageService = vipPackageService;
        }

        [HttpGet]
        [Route("GetAllVipPackages")]
        public async Task<IActionResult> GetAllVipPackages()
        {
            var listpackage = await _vipPackageService.GetAllVipPackages();
            if (listpackage == null)
            {
                return StatusCode(StatusCodes.Status404NotFound);
            }
            return Ok(listpackage);
        }

        [HttpGet]
        [Route("GetVipPackageById/{id:guid}")]
        public async Task<IActionResult> GetVipPackageById(Guid id)
        {
            var package = await _vipPackageService.GetVipPackageById(id);
            if (package == null)
            {
                return StatusCode(StatusCodes.Status404NotFound);
            }
            return Ok(package);
        }

        [HttpPost]
        [Route("AddVipPackage")]
        public async Task<IActionResult> AddVipPackage(VipPackageAddModel model, string userId)
        {
            var response = await _vipPackageService.AddVipPackage(model, userId);
            if (response == true)
            {
                return StatusCode(StatusCodes.Status200OK);
            }
            else
                return StatusCode(StatusCodes.Status500InternalServerError);
        }

        [HttpPut]
        [Route("UpdateVipPackage/{idpackage:guid}&&{userId}")]
        public async Task<IActionResult> UpdateVipPackage(VipPackageUpdateModel model, Guid idpackage, string userId)
        {
            var response = await _vipPackageService.UpdateVipPackage(model, idpackage, userId);
            if (response == true)
            {
                return StatusCode(StatusCodes.Status200OK);
            }
            else
                return StatusCode(StatusCodes.Status500InternalServerError);
        }

        [HttpDelete]
        [Route("DeleteVipPackage/{id:guid}")]
        public async Task<IActionResult> DeleteVipPackage(Guid id)
        {
            var response = await _vipPackageService.DeleteVipPackage(id);
            if (response == true)
            {
                return Ok(response);
            }
            else
                return StatusCode(StatusCodes.Status404NotFound);
        }
    }
}
using GraduateProject.Common.DTOs.Lookups;
using GraduateProject.Common.Enums;
using GraduateProject.Common.Extentions;
using GraduateProject.Common.Services.Lookups;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace GraduateProject.CP.Controllers
{
    //[Authorize(Roles = "Admin")]
    [Route("api/[controller]")]
    [ApiController]
    public class RegionController : Controller
    {
        private readonly ILookupsCRUDService _lookupsCRUDService;
        public RegionController(ILookupsCRUDService lookupsCRUDService)
        {
            _lookupsCRUDService = lookupsCRUDService;
        }

        [HttpGet]
        [Route("[action]")]
        public async Task<ActionResult<IEnumerable<List<SysRegionDTO>>>> GetSysRegions()
        {
            try
            {
                var result = await _lookupsCRUDService.GetSysRegions();
                return Json(new ResponseResult(ResponseType.Success, result));
            }
            catch (Exception ex)
            {
                return Json(new ResponseResult(ResponseType.Error, ex.GetError()));
            }
        }

        [HttpGet]
        [Route("[action]")]
        public async Task<ActionResult<IEnumerable<List<SysRegionDTO>>>> RegionList()
        {
            try
            {
                var result = await _lookupsCRUDService.RegionList();
                return Json(new ResponseResult(ResponseType.Success, result));
            }
            catch (Exception ex)
            {
                return Json(new ResponseResult(ResponseType.Error, ex.GetError()));
            }
        }

        [HttpGet]
        [Route("[action]/{id}")]
        public async Task<ActionResult<SysRegionDTO>> GetRegionById(int id)
        {
            try
            {
                var result = await _lookupsCRUDService.GetRegionById(id);
                if (result == null)
                    return Json(new ResponseResult(ResponseType.Error, result.ToString()));

                return Json(new ResponseResult(ResponseType.Success, result));
            }
            catch (Exception ex)
            {
                return Json(new ResponseResult(ResponseType.Error, ex.GetError()));
            }

        }

        [HttpPut]
        [Route("[action]/{id}")]
        public async Task<IActionResult> UpdateRegion(SysRegionDTO sysRegionDTO)
        {
            try
            {
                if (!ModelState.IsValid)
                    return Json(new ResponseResult(ResponseType.ModelNotValid, "Model State is Not Valid"));

                var result = await _lookupsCRUDService.UpdateRegion(sysRegionDTO);
                if (result == false)
                    return Json(new ResponseResult(ResponseType.Error, "The result is null!!"));

                return Json(new ResponseResult(ResponseType.Success, result));

            }
            catch (Exception ex)
            {
                return Json(new ResponseResult(ResponseType.Error, ex.GetError()));
            }
        }

        [HttpPost]
        [Route("[action]")]
        public async Task<ActionResult> AddRegion(SysRegionDTO sysRegionDTO)
        {
            try
            {
                if (!ModelState.IsValid)
                    return Json(new ResponseResult(ResponseType.ModelNotValid, "Model State is Not Valid"));

                var result = await _lookupsCRUDService.AddRegion(sysRegionDTO);
                if (result == null)
                    return Json(new ResponseResult(ResponseType.Error, "The result is null!!"));

                return Json(new ResponseResult(ResponseType.Success, result));

            }
            catch (Exception ex)
            {
                return Json(new ResponseResult(ResponseType.Error, ex.GetError()));
            }
        }


        [HttpDelete]
        [Route("[action]/{id}")]
        public async Task<ActionResult<SysRegionDTO>> DeleteRegion(int id)
        {
            try
            {
                var result = await _lookupsCRUDService.DeleteRegion(id);
                if (result == false)
                    return Json(new ResponseResult(ResponseType.Error, "There is an error with the result"));

                return Json(new ResponseResult(ResponseType.Success, result));

            }
            catch (Exception ex)
            {
                return Json(new ResponseResult(ResponseType.Error, ex.GetError()));
            }

        }
    }
}
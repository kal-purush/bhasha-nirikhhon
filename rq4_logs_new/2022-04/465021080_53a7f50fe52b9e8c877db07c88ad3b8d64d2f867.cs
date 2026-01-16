using GraduateProject.Common.DTOs.Lookups;
using GraduateProject.Common.Enums;
using GraduateProject.Common.Extentions;
using GraduateProject.Common.Models.SysModels;
using GraduateProject.Common.Services.Lookups;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace GraduateProject.CP.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CityController : Controller
    {
        private readonly ILookupsCRUDService _lookupsCRUDService;
        public CityController(ILookupsCRUDService lookupsCRUDService)
        {
            _lookupsCRUDService = lookupsCRUDService;
        }

        [HttpGet]
        [Route("[action]")]
        public async Task<ActionResult<IEnumerable<List<SysCity>>>> GetSysCities()
        {
            try
            {
                var result = await _lookupsCRUDService.GetSysCities();
                return Json(new ResponseResult(ResponseType.Success, result));
            }
            catch (Exception ex)
            {
                return Json(new ResponseResult(ResponseType.Error, ex.GetError()));
            }
        }

        [HttpGet]
        [Route("[action]")]
        public async Task<ActionResult<IEnumerable<List<SysCity>>>> CityList()
        {
            try
            {
                var result = await _lookupsCRUDService.CityList();
                return Json(new ResponseResult(ResponseType.Success, result));
            }
            catch (Exception ex)
            {
                return Json(new ResponseResult(ResponseType.Error, ex.GetError()));
            }
        }

        [HttpGet] 
        [Route("[action]/{id}")]
        public async Task<ActionResult<SysCityDTO>> GetCityById(int id)
        {
            try
            {
                var result = await _lookupsCRUDService.GetCityById(id);
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
        public async Task<IActionResult> UpdateCity(SysCityDTO sysCityDTO)
        {
            try
            {
                if (!ModelState.IsValid)
                    return Json(new ResponseResult(ResponseType.ModelNotValid, "Model State is Not Valid"));

                var result = await _lookupsCRUDService.UpdateCity(sysCityDTO);
                if (result == false)
                    return Json(new ResponseResult(ResponseType.Error, "There is an error with the result"));

                return Json(new ResponseResult(ResponseType.Success, result));

            }
            catch (Exception ex)
            {
                return Json(new ResponseResult(ResponseType.Error, ex.GetError()));
            }
        }

        [HttpPost]
        [Route("[action]")]
        public async Task<ActionResult> AddCity(SysCityDTO sysCityDTO)
        {
            try
            {
                if (!ModelState.IsValid)
                    return Json(new ResponseResult(ResponseType.ModelNotValid, "Model State is Not Valid"));

                var result = await _lookupsCRUDService.AddCity(sysCityDTO);
                if (result == null)
                    return Json(new ResponseResult(ResponseType.Error, "There is an error with the result"));

                return Json(new ResponseResult(ResponseType.Success, result));

            }
            catch (Exception ex)
            {
                return Json(new ResponseResult(ResponseType.Error, ex.GetError()));
            }
        }


        [HttpDelete]
        [Route("[action]/{id}")]
        public async Task<ActionResult<SysCityDTO>> DeleteCity(int id)
        {
            try
            {
                var result = await _lookupsCRUDService.DeleteCity(id);
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
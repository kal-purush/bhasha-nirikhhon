using CRM_api.Services.IServices;
using Microsoft.AspNetCore.Mvc;

namespace CRM_api.Controllers
{
    [ApiController]
    [Route("[controller]/[action]")]
    public class RegionController : ControllerBase
    {
        private readonly IRegionService _regionService;

        public RegionController(IRegionService regionService)
        {
            _regionService = regionService;
        }

        [HttpGet]
        #region Get All Countries
        public async Task<IActionResult> GetCountries()
        {
            try
            {
                var countries = await _regionService.GetCountriesAsync();

                return Ok(countries);
            }
            catch (Exception)
            {
                throw;
            }
        }
        #endregion

        [HttpGet]
        #region Get All States By Country
        public async Task<IActionResult> GetStatesByCountry(int countryId)
        {
            try
            {
                var states = await _regionService.GetstateByCountry(countryId);

                return Ok(states);
            }
            catch (Exception)
            {
                throw;
            }
        }
        #endregion

        [HttpGet]
        #region Get All Cities By State
        public async Task<IActionResult> GetcitiesByState(int stateId)
        {
            try
            {
                var cities = await _regionService.GetCityByState(stateId);

                return Ok(cities);
            }
            catch (Exception)
            {
                throw;
            }
        }
        #endregion
    }
}
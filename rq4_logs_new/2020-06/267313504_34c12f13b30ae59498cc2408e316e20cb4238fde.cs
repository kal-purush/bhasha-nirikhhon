using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using FAMS_AngularApi.Models.City;
using System.Threading.Tasks;

namespace FAMS_AngularApi.Controllers
{
    public class CityController : ApiController
    {
        // GET: City
        //public ActionResult Index()
        //{
        //    return View();
        //}

        CityDataAccessLayer objCountry = new CityDataAccessLayer();
        [HttpGet]
        [Route("api/City/BindState/{CountryCode}")]
        public Dictionary<string, object> BindState(string CountryCode)
        {
            return objCountry.BindState(CountryCode);
        }

        [HttpGet]
        [Route("api/City/BindCity")]
        public Dictionary<string, object> BindCity()
        {
            return objCountry.BindCity();
        }




        [HttpPost]
        [Route("api/City/AddCity/{UserId}")]
        public IEnumerable<City> AddCity([FromBody] City City, string UserId)
        {
            return objCountry.AddCity(City, UserId);
        }


        [HttpPost]
        [Route("api/City/UpdateCity/{UserId}/{CityId}")]
        public IEnumerable<City> UpdateCity([FromBody] City City, string UserId, string CityId)
        {
            return objCountry.UpdateCity(City, UserId, CityId);
        }


    }
}
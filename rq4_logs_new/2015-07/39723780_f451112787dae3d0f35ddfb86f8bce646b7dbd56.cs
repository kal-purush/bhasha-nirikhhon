using ShareWealth.Web.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Newtonsoft.Json;
using ShareWealth.Web.Models;

namespace ShareWealth.Web.Controllers
{
    public class PriceDataController : ApiController
    {
        private DataService _data;
        public PriceDataController()
        {
            _data = new DataService();
        }

        // GET: api/PriceData
        public IEnumerable<PriceData> Get()
        {
            var theDate = DateTime.Today;
            var priceData = new List<PriceData> { 
                new PriceData {Date = theDate, Open = 121, High = 138, Low=117, Close = 127},
                new PriceData {Date = theDate.AddDays(1), Open = 122, High = 135, Low=120, Close = 130},
            };
            
            return _data.GetPriceData(1);
        }

        // GET: api/PriceData/5
        public string Get(int id)
        {
            return "value";
        }

        // POST: api/PriceData
        public void Post([FromBody]string value)
        {
        }

        // PUT: api/PriceData/5
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE: api/PriceData/5
        public void Delete(int id)
        {
        }
    }
}
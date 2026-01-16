using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace PriceCompEngineAPI.Controllers
{
    public class MoreItemsController : ApiController
    {
        [HttpGet]
        public List<KeyValuePair<string, double>> GetSearchResultsSingle([FromUri] string[] itemNames)
        {
            ItemsController controller = new ItemsController();

            List<KeyValuePair<string, double>> result = new List<KeyValuePair<string, double>>();
            foreach (string itemName in itemNames)
            {
                Dictionary<string, double> search = controller.GetSearchResults(itemName);
                string key = search.Keys.First();
                double value = search.Values.First();
                result.Add(new KeyValuePair<string, double>(key, value));
            }


            return result;
        }
    }
}
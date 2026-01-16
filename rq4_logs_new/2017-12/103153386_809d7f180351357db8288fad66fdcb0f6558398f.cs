using DataBase;
using Logic;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Http;

namespace PriceCompEngineAPI.Controllers
{


    public class TopShopsController : ApiController
    {
        private IDBController _controller;

        public TopShopsController(IDBController controller)
        {
            _controller = controller;
        }

        public Dictionary<string, int> Get(int days)
        {
            TopItems topItems = new TopItems(_controller);
            Dictionary<string, int> items = topItems.GetTopShops(days);

            if (!(items.Any(i => i.Key.ToLower().Contains("rimi")))){
                items.Add("Rimi", 0);
            }

            if (!items.Any(i => i.Key.ToLower().Contains("norfa")))
            {
                items.Add("Norfa", 0);
            }

            if (!items.Any(i => i.Key.ToLower().Contains("maxima")))
            {
                items.Add("Maxima", 0);
            }

            if (!items.Any(i => i.Key.ToLower().Contains("iki")))
            {
                items.Add("Iki", 0);
            }

            if (!items.Any(i => i.Key.ToLower().Contains("lidl")))
            {
                items.Add("Lidl", 0);
            }

            items.OrderBy(o => o.Key).ThenBy(o => o.Value);

            return items;
        }
    }
}
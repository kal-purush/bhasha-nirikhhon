using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Microsoft.Extensions.Configuration;
using System.Linq;
using System.Globalization;
using YahooFinance.Controllers;
using YahooFinance.Models;

namespace YahooFinance.Controllers
{
    public class StockController : Controller
    {
        [HttpGet]
        [Route("stock/{symbol}")]
        public IActionResult ViewStock(string symbol)
        {
            Dictionary<string,string> Stock = new Dictionary<string,string>();
            WebRequest.IndyStockInfo(symbol, JsonResult =>
                {
                    Stock = JsonResult;
                }
            ).Wait();

            ViewBag.Stock = Stock;
            return View();
        }

        [HttpPost]
        [Route("search")]
        public IActionResult Search(string symbol)
        {
            return Redirect($"/stock/{symbol}");
        }
    }
}
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Microsoft.Extensions.Configuration;
using System.Linq;

namespace YahooFinance.Controllers
{
    public class PortfolioController : Controller
    {
        [HttpGet]
        [Route("portfolio")]
        public IActionResult Portfolio()
        {

            return View();
        }

        [HttpGet]
        [Route("purchase")]
        public IActionResult Purchase(string Symbol)
        {
            var Quote = new Dictionary<string, string>();
            WebRequest.IndyStockInfo(Symbol, JsonResponse =>
                {
                    Quote = JsonResponse;
                }
            ).Wait();

            System.Console.WriteLine(Quote["symbol"]);
            System.Console.WriteLine(Quote["companyName"]);
            System.Console.WriteLine(Quote["latestPrice"]);

            double price = Convert.ToDouble(Quote["latestPrice"]);

            var Buy = price + 600.00;
            System.Console.WriteLine(Buy);
            double Shares = Buy / price;
            System.Console.WriteLine(Shares);
            double Value = Shares * price;
            System.Console.WriteLine(Value);

            return View();
        }
    }
}
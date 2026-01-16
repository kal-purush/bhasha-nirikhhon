using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using ChajdPizzaWebApp.Models;

namespace ChajdPizzaWebApp.Controllers
{
    public class SpecialtyPizzaController : Controller
    {
        public async Task<IActionResult> Order(int? id)
        {
            if (id is null)
            {
                return NotFound();
            }
            var Username = User.Identity.Name;
            Customer customer = new Customer();
            IEnumerable<Orders> orders = null;

            SpecialtyPizza specialtyPizza = new SpecialtyPizza();
           
            using (var client = new HttpClient())
            {
                client.BaseAddress = new Uri("https://chajdpizza.azurewebsites.net/api/");

                client.DefaultRequestHeaders.Clear();
                client.DefaultRequestHeaders.Accept.Add
                    (new MediaTypeWithQualityHeaderValue("application/json"));
                //Get SpecialtyPizzaDetails
                HttpResponseMessage ResP = await client.GetAsync("PizzaTypesApi/special/" + id);

                if (ResP.IsSuccessStatusCode)
                {
                    var SpecialRes = ResP.Content.ReadAsStringAsync().Result;

                    specialtyPizza = JsonConvert.DeserializeObject<SpecialtyPizza>(SpecialRes);
                }
                //GetCustomerId
                client.DefaultRequestHeaders.Clear();
                client.DefaultRequestHeaders.Accept.Add
                    (new MediaTypeWithQualityHeaderValue("application/json"));
                HttpResponseMessage ResC = await client.GetAsync("CustomersApi/ByUser/Customer01@aol.com");// + Username);

                if (ResC.IsSuccessStatusCode)
                {
                    var customerRes = ResC.Content.ReadAsStringAsync().Result;

                    customer = JsonConvert.DeserializeObject<Customer>(customerRes);
                }


            }
            


            return View("../Customers/Details" , customer);
        }
    }
}
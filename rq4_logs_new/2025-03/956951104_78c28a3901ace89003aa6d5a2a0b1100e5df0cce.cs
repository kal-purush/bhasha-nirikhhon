using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using WebApp.Common.Utility.Interface;
using WebApp.Model.DTO.Customer;

namespace WebApp.Web.Controllers
{
    public class CustomerController : Controller
    {
        private readonly IHttpClientUtility _httpClientUtility;
        private readonly IConfiguration _configuration;
        private readonly string _uri;
        public CustomerController(IHttpClientUtility httpClientUtility, IConfiguration configuration)
        {
            _httpClientUtility = httpClientUtility;
            _configuration = configuration;
            _uri = _configuration.GetValue<string>("ApiHost") + "Customer" ?? string.Empty;
        }

        // GET: CustomerController
        public async Task<ActionResult> Index()
        {
            var customersStr = await _httpClientUtility.GetAsync(_uri);
            var customers = JsonConvert.DeserializeObject<List<CustomerDto>>(customersStr);
            return View(customers);
        }

        // GET: CustomerController/Details/5
        public async Task<ActionResult> Details(string id)
        {
            var uri = _uri + "/" + id;
            var customerStr = await _httpClientUtility.GetAsync(uri);
            var customer = JsonConvert.DeserializeObject<CustomerDto>(customerStr);
            return View(customer);
        }

        // GET: CustomerController/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: CustomerController/Create
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> Create(IFormCollection collection)
        {
            try
            {
                string customerID = await _httpClientUtility.PostAsync(_uri, collection);
                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return View();
            }
        }

        // GET: CustomerController/Edit/5
        public async Task<ActionResult> Edit(string id)
        {
            var uri = _uri + "/" + id;
            var customerStr = await _httpClientUtility.GetAsync(uri);
            var customer = JsonConvert.DeserializeObject<CustomerDto>(customerStr);
            return View(customer);
        }

        // POST: CustomerController/Edit/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> Edit(string id, IFormCollection collection)
        {
            var uri = _uri + "/" + id;
            try
            {
                bool isSuccess = await _httpClientUtility.PutAsync(uri, collection);
                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return Redirect("/Customer/Edit/" + id);
            }
        }

        // DELETE: CustomerController/Delete/5
        public async Task<ActionResult> Delete(string id)
        {
            var uri = _uri + "/" + id;
            var customerStr = await _httpClientUtility.GetAsync(uri);
            var customer = JsonConvert.DeserializeObject<CustomerDto>(customerStr);
            return View(customer);
        }

        // POST: CustomerController/Delete/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> DeleteItem(string id)
        {
            var uri = _uri + "/" + id;
            try
            {
                bool isSuccess = await _httpClientUtility.DeleteAsync(uri);
                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return Redirect("/Customer/Delete/" + id);
            }
        }
    }
}
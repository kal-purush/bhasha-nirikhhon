using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Web;
using System.Web.Mvc;
using System.Threading.Tasks;

using OneRing.Models;
using Newtonsoft.Json;

namespace OneRing.Controllers
{
    public class TodoPortletController : Controller
    {
        // GET: TodoPortlet
        public async Task<ActionResult> Index()
        {
			List<ToDoItem> todos;
			string requestUrl = "http://todostaticsource20171107050947.azurewebsites.net/api/ToDoItems";
			HttpClient client = new HttpClient();
			HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, requestUrl);
			HttpResponseMessage response = await client.SendAsync(request);

			if (response.IsSuccessStatusCode) {
				// pass
				string responseString = await response.Content.ReadAsStringAsync();
				todos = JsonConvert.DeserializeObject<List<ToDoItem>>(responseString);
			} else {
				todos = new List<ToDoItem>();
				ViewBag.ErrorMessage = "UnexpectedError";
			}

			return View(todos);
        }
    }
}
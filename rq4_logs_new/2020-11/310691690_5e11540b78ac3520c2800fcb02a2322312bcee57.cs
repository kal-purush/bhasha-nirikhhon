using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using GGsWeb.Models;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using NpgsqlTypes;

namespace GGsWeb.Controllers
{
    public class OrderController : Controller
    {
        const string url = "https://localhost:44316/";
        private User user;
        public IActionResult Reciept()
        {
            return View();
        }
        public IActionResult AddOrder(Cart cart)
        {
            // Get User
            user = HttpContext.Session.GetObject<User>("User");
            if (user == null)
                return RedirectToAction("Login", "Home");

            // Ensure model state
            if (ModelState.IsValid)
            {
                // Create new order object and set values
                Order order = new Order();
                order.totalCost = cart.totalCost;
                order.locationId = user.locationId;
                order.orderDate = DateTime.Now;
                NpgsqlDateTime npgsqlDateTime = order.orderDate; // Used for API call
                order.userId = user.id;

                // Set up API calls
                using (var client = new HttpClient())
                {
                    client.BaseAddress = new Uri(url);

                    // Serialize order objecto to be added
                    var json = JsonConvert.SerializeObject(order);
                    var data = new StringContent(json, Encoding.UTF8, "application/json");

                    // Use POST method to add to DB
                    var response = client.PostAsync("order/add", data);
                    response.Wait();

                    while (response.Result.IsSuccessStatusCode)
                    {
                        // Added new order successfully
                        // Now get order we just added to map orderId
                        response = client.GetAsync($"order/get?dateTime={npgsqlDateTime}");
                        response.Wait();
                        var result = response.Result.Content.ReadAsStringAsync();
                        var newOrder = JsonConvert.DeserializeObject<Order>(result.Result);

                        // Got order back successfully
                        order.id = newOrder.id;
                        foreach (var item in user.cart.cartItems)
                        {
                            // For each item in cart, map to LineItem object
                            VideoGame videoGame = item.videoGame;
                            LineItem lineItem = new LineItem();
                            lineItem.orderId = order.id;
                            lineItem.videoGameId = item.videoGameId;
                            lineItem.cost = videoGame.cost;
                            lineItem.quantity = item.quantity;

                            // Serialize LineItem object and add to db using POST method
                            json = JsonConvert.SerializeObject(lineItem);
                            data = new StringContent(json, Encoding.UTF8, "application/json");
                            response = client.PostAsync("lineitem/add", data);
                            response.Wait();

                            // Added new line item successfully
                            // Get inventory item and update quantity
                            response = client.GetAsync($"inventoryitem/get/{user.locationId}/{item.videoGameId}");
                            response.Wait();
                            result = response.Result.Content.ReadAsStringAsync();
                            var inventoryItem = JsonConvert.DeserializeObject<InventoryItem>(result.Result);
                            inventoryItem.quantity -= item.quantity;

                            json = JsonConvert.SerializeObject(inventoryItem);
                            data = new StringContent(json, Encoding.UTF8, "application/json");
                            response = client.PutAsync("inventoryitem/update", data);
                            response.Wait();
                        }

                        // Clear cart items and redirect to receipt view
                        user.cart.cartItems.Clear();
                        HttpContext.Session.SetObject("User", user);
                        return RedirectToAction("GetInventory", "Customer"); // TODO: make receipt view
                    }
                }
            }
            return RedirectToAction("GetInventory", "Customer");
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Food.core;
using Food.data;

namespace Food.Pages.Restaurants
{
    public class DeleteModel : PageModel
    {
        private readonly IRestaurantData restaurantData;
        public Restaurant restaurant { get; set; }

        public DeleteModel(IRestaurantData restaurantData){
            this.restaurantData = restaurantData;
        }
        public IActionResult OnGet(int restaurantId)
        {
            restaurant = restaurantData.GetRestaurantsById(restaurantId);
            // if the restaurant is not in database 
            if(restaurant == null){
                return RedirectToPage("./NotFound");
            }
            return Page();
        }
        public IActionResult OnPost(int restaurantId)
        {
            var restaurant_ = restaurantData.Delete(restaurantId);
            restaurantData.Commit();

            if(restaurant_ == null){
                return RedirectToPage("./NotFound");
            }
            TempData["Message"] = $"{restaurant_.Name} is deleted";
            return RedirectToPage("./List");
        }
    }
}
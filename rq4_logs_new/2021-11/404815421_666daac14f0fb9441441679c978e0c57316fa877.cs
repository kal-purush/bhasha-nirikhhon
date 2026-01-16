using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using WebWasted.Models;

namespace WebWasted.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CartController : ControllerBase
    {
        [HttpPost]
        public int Post(int userID, int foodID)
        {
            using (DataContext context = new DataContext())
            {
                var findFood = (from food in context.Foods where food.ID == foodID select food).First();

                if ((findFood == null) && (userID != findFood.OwnerID))
                {
                    return -1;
                }

                findFood.BuyerID = userID;
                
                return findFood.BuyerID;
            }
        }
    }
}
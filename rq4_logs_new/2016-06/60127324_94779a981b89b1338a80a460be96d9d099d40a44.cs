using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using ToyWebsite.DAL;
using ToyWebsite.Models;

namespace ToyWebsite.Controllers
{
    //Conrols the cart page
    public class CartController : Controller
    {
        // GET: Cart
        public ActionResult Index()
        {
            //Retrieve all the information in the carts for the user
            StoreContext context = new StoreContext();
            ILookup<int, Cart> cartItems = (from i in context.Carts
                                            where i.userID == 1
                                            select i).ToLookup(x => x.itemID);


            return View(cartItems);
        }

        /*Removes all items of the same itemID in a cart*/
        [HttpPost]
        public ActionResult RemoveItem(int anItemID)
        {
            StoreContext context = new StoreContext();
            List<Cart> removeItems = (from c in context.Carts
                                      where c.itemID == anItemID
                                      select c).ToList();

            foreach (Cart x in removeItems)
            {
                context.Carts.Remove(x);
            }
            context.SaveChanges();

            return RedirectToAction("Index");
        }

        [HttpPost]
        public ActionResult IncrementItem(int anItemID)
        {
            StoreContext context = new StoreContext();

            if ((from c in context.Carts
                 where c.itemID == anItemID && c.userID == 1
                 select c).Count() < 10)
            {
                context.Carts.Add(new Cart { itemID = anItemID, userID = 1 });
                context.SaveChanges();
            }

            return RedirectToAction("Index");

        }

        [HttpPost]
        public ActionResult DecrementItem(int anItemID)
        {
            StoreContext context = new StoreContext();

            context.Carts.Remove((from c in context.Carts
                                  where c.itemID == anItemID && c.userID == 1
                                  select c).First());
            context.SaveChanges();

            return RedirectToAction("Index");

        }
    }
}
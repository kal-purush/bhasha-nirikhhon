using Microsoft.AspNet.Identity;
using OnlineShop.Models;
using OnlineShop.ViewModels;
using PagedList;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace OnlineShop.Controllers
{
    public class AccountDetailsController : Controller
    {
        // GET: AccountDetails

        ApplicationDbContext db = new ApplicationDbContext();
        public ActionResult Index()
        {
           
            return View();
        }


        public ActionResult OrderHistory(int? page)
        {
            string userId = User.Identity.GetUserId().ToString();
            var orderHistory = from order in db.Sales
                               where order.UserId.Equals(userId)
                               select new OrderHistoryViewModel { PurchasedOn = order.Date, Total = order.Total,SaleId =order.SaleId,SalesItems = order.SalesItems};
            return View(orderHistory.Where(o => o.PurchasedOn.Month == 11).OrderBy(o => o.SaleId).OrderByDescending(o => o.SaleId).ToList().ToPagedList(page ?? 1, 2));
        }
       
        public ActionResult AccountInfo()
        {

            string userId = User.Identity.GetUserId().ToString();
            var customer = from cust in db.Customers
                           where cust.ApplicationUserId.Equals(userId)
                           select new CustomerViewModel
                           {
                                FirstName = cust.FirstName,
                                LastName = cust.LastName,
                                CellNo = cust.CellNo,
                                Address = cust.Address,
                                City = cust.City,
                                Province = cust.Province,
                                ZipCode = cust.ZipCode
                           };
            return View(customer.ToList());
        }
    }
}
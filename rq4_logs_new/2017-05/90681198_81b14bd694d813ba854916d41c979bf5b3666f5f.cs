using BangazonSiteMVC.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace BangazonSiteMVC.Controllers
{
    public class OrderController : Controller
    {
 
        readonly IOrderRepository _orderRepository;
        public OrderController(IOrderRepository orderRepository)
        {
            _orderRepository = orderRepository;
        }


        // GET: Order
        public ActionResult GetAnOrder(Order newOrder)
        {
            return View();
        }
    }
}
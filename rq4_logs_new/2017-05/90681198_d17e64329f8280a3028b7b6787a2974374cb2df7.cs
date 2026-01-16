using BangazonSiteMVC.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace BangazonSiteMVC.Controllers
{
    public class OrderLineItemRepository : Controller
    {

        readonly IOrderLineItemRepository _orderLineItemRepository;

        public OrderLineItemRepository(IOrderLineItemRepository orderLineItemRepository)
        {
            _orderLineItemRepository = orderLineItemRepository;
        }

        // GET: OrderLineItemRepository
        public ActionResult Index()
        {
            return View();
        }

        // GET: OrderLineItemRepository/Details/5
        public ActionResult GetOrderLineItem(int OrderLineItem)
        {
            return View();
        }

        // GET: OrderLineItemRepository/Create
        public ActionResult AddOrderLineItem(OrderLineItem newOrderLineitem)
        {
            _orderLineItemRepository.Save(newOrderLineitem);

            return View();
        }

        // POST: OrderLineItemRepository/Create
        [HttpPost]
        public ActionResult Create(FormCollection collection)
        {
            try
            {
                // TODO: Add insert logic here

                return RedirectToAction("Index");
            }
            catch
            {
                return View();
            }
        }

        // GET: OrderLineItemRepository/Edit/5
        public ActionResult Edit(int id)
        {
            return View();
        }

        // POST: OrderLineItemRepository/Edit/5
        [HttpPost]
        public ActionResult Edit(int id, FormCollection collection)
        {
            try
            {
                // TODO: Add update logic here

                return RedirectToAction("Index");
            }
            catch
            {
                return View();
            }
        }

        // GET: OrderLineItemRepository/Delete/5
        public ActionResult Delete(int id)
        {
            return View();
        }

        // POST: OrderLineItemRepository/Delete/5
        [HttpPost]
        public ActionResult Delete(int id, FormCollection collection)
        {
            try
            {
                // TODO: Add delete logic here

                return RedirectToAction("Index");
            }
            catch
            {
                return View();
            }
        }
    }
}
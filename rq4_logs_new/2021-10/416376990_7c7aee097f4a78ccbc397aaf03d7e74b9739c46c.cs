using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Microsoft.AspNetCore.Mvc;
using PopLibrary;
using PopLibrary.Data;
using PopLibrary.SqlModels;

namespace PopApis.Controllers
{
    public class EventsController : Controller
    {
        private List<Event> events = new();
        private EventData _data;

        public EventsController(EventData data)
        {
            _data = data;
        }
        // GET: EventsController
        public ActionResult Index()
        {
            return View("Index", _data.GetEvents());
        }

        public ActionResult Edit(int id)
        {
            return View(_data.GetEvents().ToList().Find(t => t.Id == id));
        }

        // GET: EventsController/Edit
        [HttpPost]
        public ActionResult Edit(int id, Event itemToUpdate)
        {
            try
            {
                _data.AddOrUpdateEvent(itemToUpdate);
                return RedirectToAction("Index", _data.GetEvents());
            }
            catch(Exception Ex)
            {
                return RedirectToAction("Index", _data.GetEvents());
            }
        }
    }
}
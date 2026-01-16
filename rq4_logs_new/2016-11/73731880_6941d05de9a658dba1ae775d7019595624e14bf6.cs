using Nancy;
using CarDealership.Objects;
using System;
using System.Collections.Generic;

namespace CarDealership
{
    public class HomeModule : NancyModule
    {
        public HomeModule()
        {
            Get["/"] = _ => View["add_new_car.cshtml"];
            Get["/car-added"] = _ => {
                return View["car_added.cshtml"];
            };
        }
    }
}
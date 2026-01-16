using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace GoodQRma.Models
{
    public class Event
    {

        private int eventID { get; set; }
        private byte image { get; set; }
        private string name { get; set; }
        private string description { get; set; }
        private DateTime eventDate { get; set; }
        private DateTime eventTime { get; set; }

        private string address1 { get; set; }
        private string address2 { get; set; }

        private string city { get; set; }
        private string state { get; set; }
        private string zipCode { get; set; }
        private string country { get; set; }
        private string contact { get; set; }
        private string phone { get; set; }

        private float gpsLong { get; set; }
        private float gpsLat { get; set; }
        
        

        public Event()
        {
            
            image = 0;
            name = "";
            description = "";
            eventDate = DateTime.Now;
            eventTime = DateTime.Now;
            address1 = "";
            address2 = "";
            city = "";
            state = "";
            zipCode = "";
            country = "";
            contact = "";
            phone = "";
            gpsLong = 0.00F;
            gpsLat = 0.00F;
         }






    }
}
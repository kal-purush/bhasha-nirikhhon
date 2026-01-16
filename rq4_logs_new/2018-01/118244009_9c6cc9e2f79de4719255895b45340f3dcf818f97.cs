using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using Android.App;
using Android.Content;
using Android.OS;
using Android.Runtime;
using Android.Views;
using Android.Widget;

namespace Exercise_5.Models
{
    class CityList
    {
        [XmlAttribute(AttributeName = "updated")]
        public string UpdatedTime { get; set; }

        [XmlAttribute(AttributeName = "unit")]
        public string Unit { get; set; }
        
        [XmlElement("ratelist")]
        public List<City> Cities { get; set; }
    }
}
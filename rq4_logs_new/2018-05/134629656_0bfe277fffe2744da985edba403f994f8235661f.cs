using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace ReptileCollection.Models
{
    public class AnimalWeight
    {
        public int Id { get; set; }
        public int Weight { get; set; }
        public string WeightMeasurement { get; set; }
        public DateTime Date { get; set; }

        public Animal Animal { get; set; }
    }
}
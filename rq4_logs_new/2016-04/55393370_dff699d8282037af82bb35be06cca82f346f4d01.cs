using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DATACOM_RFID_MODELS.Models
{
   public class Person
    {
        public int PersonID { get; set; }
        public string Voornaam { get; set; }
        public string FamilieNaam { get; set; }
        public int Leeftijd { get; set; }
        public string Postcode { get; set; }
        public string Gemeente { get; set; }
        public string Email { get; set; }
    }
}
using System;
using System.Collections.Generic;

namespace ZorgPortalIoT.Model
{
    public partial class Patient
    {
        public Patient()
        {
            Sensor = new HashSet<Sensor>();
        }

        public int PatientId { get; set; }
        public string Voornaam { get; set; }
        public string Achternaam { get; set; }
        public int Leeftijd { get; set; }
        public string FotoUrl { get; set; }
        public bool? GeslotenKamer { get; set; }

        public virtual ICollection<Sensor> Sensor { get; set; }
    }
}
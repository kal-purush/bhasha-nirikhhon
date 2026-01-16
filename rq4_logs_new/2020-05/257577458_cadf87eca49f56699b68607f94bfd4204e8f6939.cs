using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Project_Monopoly_Models
{
    public class Instellingen
    {
        //Properties
        public int Bedrag { get; set; }
        public int Spelers { get; set; }
        public bool VerzamelenGevangenis { get; set; }
        public bool VerzamelenParking { get; set; }

        //Constructor
        private void Instelling(int bedrag, int spelers, bool gevangenis, bool parking)
        {
            Bedrag = bedrag;
            Spelers = spelers;
            VerzamelenGevangenis = gevangenis;
            VerzamelenParking = parking;
        }

    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Project_Monopoly_Models
{
    public class Settings
    {
        //Attributen
        private int _bedrag;
        private double _spelers;
        private bool _gevangenis;
        private bool _parking;

        //Properties
        public int Bedrag { get => _bedrag; set => _bedrag = value; }
        public double Spelers { get => _spelers; set => _spelers = value; }
        public bool Gevangenis { get => _gevangenis; set => _gevangenis = value; }
        public bool Parking { get => _parking; set => _parking = value; }

        //constructor
        public Settings(int bedrag, double spelers, bool gevangenis, bool parking)
        {
            Bedrag = bedrag;
            Spelers = spelers;
            Gevangenis = gevangenis;
            Parking = parking;
        }

    }
}
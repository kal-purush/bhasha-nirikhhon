using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ColdPlayProject.encapsulation
{
    public class PajeroJeep
    {

        private int _pajeeroAge = 2;
        private string _pajeeroName = "Pajeroo Jeep";
        private bool _ispajeeroStarted = true;
        private double _pajeeroPrice = 23000d;


        public int PajeroJeepAge { get { return _pajeeroAge; } set { _pajeeroAge = value; } }
        public string PajeroName { get; set; }
    }
}
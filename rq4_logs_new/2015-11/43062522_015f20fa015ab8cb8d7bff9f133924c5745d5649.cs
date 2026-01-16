using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Saab.CBRN.Wcf.DataContracts
{
    public class Event
    {
        private string _command;
        private string _sensor;
        private string _id;

        public string Command
        {
            get { return _command; }
            set { _command = value; }
        }

        public string Sensor
        {
            get { return _sensor; }
            set { _sensor = value; }
        }

        public string Id {
            get { return _id;  }
            set { _id = value; }
        }
    }
}
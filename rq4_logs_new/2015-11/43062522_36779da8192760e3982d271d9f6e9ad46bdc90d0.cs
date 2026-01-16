using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Saab.CBRN.Wcf.DataContracts
{
    class AP2Ce : SensorBase
    {
        private IEnumerable<AP2CeData> _data;
        private AP2CeState _state;

        [DataMember]
        public IEnumerable<AP2CeData> Data
        {
            get { return _data; }
            set { _data = value; }
        }

        [DataMember]
        public AP2CeState State
        {
            get { return _state; }
            set { _state = value; }
        }
    }
}
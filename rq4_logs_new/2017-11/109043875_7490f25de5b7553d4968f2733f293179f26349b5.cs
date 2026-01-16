using System.Collections.Generic;

namespace Innovation.AR
{
    public class DataModel
    {
        public DataModel()
        {

            uldEnabled = false;
            casEnabled = false;
            airdropLight = 0;
            radarAltitude = "0 ft";
            timeToDrop = "0.0 sec";
            textToSpeech = "";

            casMsgList = new List<string>();
        }

        public bool uldEnabled { get; set; }
        public bool casEnabled { get; set; }
        public int airdropLight { get; set; }

        //public string masterTime { get; set;  }
        public string radarAltitude { get; set; }

        public string timeToDrop { get; set; }

        public string textToSpeech { get; set; }
        public List<string> casMsgList { get; set; }
    }
}
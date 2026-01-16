using System;
using System.Collections.Generic;

namespace Class_Scheduler.Models
{
    public class Staff 
    {
        public String name { get; set; }

        public String id { get; set; }

        //Each day (Mon, Tues, etc maps to a DailyAvailibility object which 
        //contains all start and finish times for the staff on that day.)
        public Dictionary<String, List<int[]>> availaibility { get; set; }

        public List<String> preferences { get; set; }

        public Staff(String inName, String inID, Dictionary<String, List<int[]>> inAvailibility, List<String> inPreferences)
        {
            name = inName;
            id = inID;
            availaibility = inAvailibility;
            preferences = inPreferences;
        }
    }
}
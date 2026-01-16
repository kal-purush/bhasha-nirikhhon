using System;
using System.Collections.Generic;

namespace Class_Scheduler.Models
{
    public class Class {

        public string unitCode { get; }

        public TimePeriod datetime { get; }

        public int numTutors { get; }

        public List<Staff> allocatedTutors {get; set;}

        public Class(TimePeriod datetime, string unitCode, int numTutors)
        {
            this.datetime = datetime;
            this.unitCode = unitCode;
            this.numTutors = numTutors;
            this.allocatedTutors = new List<Staff>();
        }
        public bool needsStaff() {
            return allocatedTutors.Count < numTutors;
        }
        public override string ToString()
        {
            string timeslot = "";

            timeslot += "\n\nUnit: " + unitCode;
            timeslot += "\nNumber of Tutors Required: " + numTutors;
            timeslot += datetime.ToString();

            return timeslot;
        }
    }
}
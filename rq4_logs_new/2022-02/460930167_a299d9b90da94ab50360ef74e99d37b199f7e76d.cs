using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CoreProject_Models.Models
{
    public class Temp
    {
        public string Firstname { get; set; }
        public string Lastname { get; set; }
        public DateTime? DateOfBirth { get; set; } // Nullable because it may not have been set

        // Returns a string that contains all property values of the class object
        // Not mandatory but prepared to display model data easily.
        public override string ToString()
        {
            string dateOfBirthAsString = "Unknown!";
            if (DateOfBirth != null)
            {
                dateOfBirthAsString = DateOfBirth?.Date.ToShortDateString();
            }
            return string.Format($"Firstname: {Firstname} Lastname: {Lastname} DateOfBirth:{dateOfBirthAsString}");
        }
    }
}
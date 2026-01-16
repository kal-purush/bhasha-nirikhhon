using System;
using System.Collections.Generic;
using System.Text;

namespace EventPlanner.Models
{
    public class Organizer : User
    {
        public int Rating { get; set; }

        public Organizer(string username, string password, string firstName, string lastName)
            : base(username, password, firstName, lastName)
        {
        }

        public Organizer(string username, string password, string firstName, string lastName, int rating)
            : this(username, password, firstName, lastName)
        {
            Rating = rating;
        }
    }
}
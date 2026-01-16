using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace RentalManagement.ViewModel
{
    /*
     * Wrapper class used to pass ticket data to the view
     */
    public class TicketWrapper
    {
        public int id { get; set; }
        public DateTime issueDate { get; set; }
        public int priority { get; set; }
        public int empId { get; set; }
        public string name { get; set; }
        public string companyName { get; set;}
        public string description { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using RentalManagement.Models;

namespace RentalManagement.ViewModel
{
    public class TicketViewModel
    {
        public Ticket Ticket { get; set; }
        public ICollection<Ticket> Tickets { get; set; }
        public Employee Employee { get; set; }
        public ICollection<Employee> Employees { get; set; }
        public Contractor Contractor { get; set; }
        public ICollection<Contractor> Contractors { get; set; }
        public int empId { get; set; }
    }
}
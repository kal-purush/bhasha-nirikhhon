using System;
using System.Collections.Generic;
using System.Text;

namespace Auction.LMS.CORE.DTO
{
   public class RegistorDTO
    {
        public string Fname { get; set; }
        public string Lname { get; set; }
        public string Email { get; set; }
        public string Phone { get; set; }

        public string Country { get; set; }
        public string City { get; set; }
        public string Street { get; set; }
        public string ImageName { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public int DeptId { get; set; }
        public int CustomerId { get; set; }
        public int EmployeeId { get; set; }


    }
}
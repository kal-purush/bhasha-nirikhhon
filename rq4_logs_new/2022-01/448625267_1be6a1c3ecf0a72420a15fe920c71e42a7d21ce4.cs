using System;
using System.Collections.Generic;

#nullable disable

namespace WebShop.Models
{
    public partial class Tblcustomer
    {
        public string CustomerId { get; set; }
        public string CustomerName { get; set; }
        public string CustomerFathername { get; set; }
        public string CustomerCnic { get; set; }
        public string CustomerMobileno { get; set; }
        public string CustomerAddress { get; set; }
        public string CustomerReference { get; set; }
        public DateTime? CustomerCreationdate { get; set; }
    }
}
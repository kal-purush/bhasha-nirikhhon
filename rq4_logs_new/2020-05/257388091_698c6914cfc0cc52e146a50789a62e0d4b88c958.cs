using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.EntityFrameworkCore;

namespace BitsAndBobs.Models
{
    public class Customer
    {
        public int CustomerID { get; set; }

		private String custFirstName;
		public String CustFirstName
		{
			get { return custFirstName; }
			set { custFirstName = value; }
		}

		private String custLastName;

		public String CustLastName
		{
			get { return custLastName; }
			set { custLastName = value; }
		}

		public Customer() { }
	}
}
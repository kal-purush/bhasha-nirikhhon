using System;
using System.ComponentModel.DataAnnotations;

namespace Project0
{
    public class Customer
    {   
        private Guid customerId = Guid.NewGuid(); //Creates a new Guid For CustomerId
        [Key]//Sets the below to be the key for the database
        public Guid CustomerId { get{return customerId;} set {customerId = value;}} //Sets a public Guid to the above guid and returns the value
        
  
         private string uname;
         public string Uname
        {
            get { return uname; }
            set
            {
                if (value is string && value.Length < 20 && value.Length > 0)
                {
                    uname = value;
                }
                else
                {
                    throw new Exception("This is not a valid username");
                }
            }
        }

         private string fname;
         public string Fname
        {
            get { return fname; }
            set
            {
                if (value is string && value.Length < 20 && value.Length > 0)
                {
                    fname = value;
                }
                else
                {
                    throw new Exception("This is not a valid name");
                }
            }
        }

        private string lname;
        public string Lname
        {
            get { return lname; }
            set
            {
                if (value is string && value.Length < 20 && value.Length > 0)
                {
                    lname = value;
                }
                else
                {
                    throw new Exception("This is not a valid name");
                }
            }
        }
        
    
    
    }
}
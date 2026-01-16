using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text;

namespace Lunabank.Data.Models
{
 public   class AccountModel
    {
        public Guid AccountId { get; set; }
  
        public string AccountNumber { get; set; }
    
        public DateTime CreatedOn { get; set; }
   
        public string AccountType { get; set; }
     
        public string Status { get; set; }
      
        public decimal Balance { get; set; }

        public string FirstName { get; set; }
        public string LastName { get; set; }

    }
}
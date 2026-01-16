using DiamondStoreSystem.Common.Enum;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DiamondStoreSystem.DTO.EntitiesRequest.Account
{
    public class AccountAllField
    {
        public string AccountID { get; set; }
        public string Email { get; set; }
        public string LastName { get; set; }
        public string FirstName { get; set; }
        public decimal Phone { get; set; }
        public string Address { get; set; }
        public Gender Gender { get; set; }
        public DateTime DOB { get; set; }
        public DateTime JoinDate { get; set; }
        public int? LoyaltyPoint { get; set; }
        public bool Block { get; set; }
        public Role Role { get; set; }
        public WorkingSchedule WorkingSchedule { get; set; }
    }
}
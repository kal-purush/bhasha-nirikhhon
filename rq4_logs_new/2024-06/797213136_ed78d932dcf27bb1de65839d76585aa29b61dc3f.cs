using DiamondStoreSystem.Common.Enum;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Security.Principal;

namespace DiamondStoreSystem.DTO.EntitiesResponse.Account
{
    public class AccountEmployeeResponse
    {
        public string AccountID { get; set; }
        public string Email { get; set; }
        public string Password { get; set; }
        public string LastName { get; set; }
        public string FirstName { get; set; }
        public int Phone { get; set; }
        public string Address { get; set; }
        public string Gender { get; set; }
        public DateTime DOB { get; set; }
        public DateTime JoinDate { get; set; }
        public WorkingSchedule WorkingSchedule { get; set; }
    }
}
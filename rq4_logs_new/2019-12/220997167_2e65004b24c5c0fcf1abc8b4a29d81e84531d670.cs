using Microsoft.AspNetCore.Identity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace WebFarmacia.Web.Data.Entities
{
    public class User:IdentityUser
    {
        public string FirstName { get; set; }


        public string LasName { get; set; }
    }
}
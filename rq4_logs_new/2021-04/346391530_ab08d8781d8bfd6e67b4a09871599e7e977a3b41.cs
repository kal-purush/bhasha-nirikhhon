using Microsoft.AspNetCore.Identity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace StartSpelerMVC.Areas.Identity.Data
{
    public class CustomerUser:IdentityUser
    {
        [PersonalData]
        public string Voornaam { get; set; }
        [PersonalData]
        public string Achternaam { get; set; }
        [PersonalData]
        public string Gebruikersnaam { get; set; }
        [PersonalData]
        public DateTime Geboortedatum { get; set; }
    }
}
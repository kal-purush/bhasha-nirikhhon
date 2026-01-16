using System;
using System.Collections.Generic;

#nullable disable

namespace DataAccess.Models
{
    public partial class Login
    {
        public Login()
        {
            SurveyTakens = new HashSet<SurveyTaken>();
            Surveylists = new HashSet<Surveylist>();
        }

        public int LoginId { get; set; }
        public string LoginUserName { get; set; }
        public string PasswordHash { get; set; }
        public int? Role { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Email { get; set; }
        public string UserAddress { get; set; }

        public virtual RoleType RoleNavigation { get; set; }
        public virtual ICollection<SurveyTaken> SurveyTakens { get; set; }
        public virtual ICollection<Surveylist> Surveylists { get; set; }
    }
}
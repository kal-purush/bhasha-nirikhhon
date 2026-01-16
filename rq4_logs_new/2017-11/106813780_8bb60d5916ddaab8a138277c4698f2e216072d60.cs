using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Common.Utilities;
using System.Web.Mvc;

namespace WebSetting.Models
{
    public class UserModel
    {
        public long UserId { get; set; }
        public string Username { get; set; }
        public string Psswd { get; set; }
        public string ConfirmPsswd { get; set; }
        public string PrefixName { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Gender { get; set; }
        public string OrganizeName { get; set; }
        public string DepartmentName { get; set; }
        public string PositionName { get; set; }
        public DateTime? LastLoginTime { get; set; }
        public string LastLoginTimeDisplay
        {
            get {
                return LastLoginTime.FormatDateTime(Constants.DateTimeFormat.DefaultFullDateTime);
            }

        }
        public bool ForceChangePsswd { get; set; }
        public bool ActiveStatus { get; set; }
        //public SelectList ActiveStatusList { get; set; }
        public SelectList PrefixNameSelectList { get; set; }
    }
}
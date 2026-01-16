using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MRF.DataAccess.Data
{
    public class Utility
    {

        public int GetRoleValue(string roleName)
        {
            switch (roleName.ToLower())
            {
                case "superadmin":
                    return 1;
                case "admin":
                    return 2;
                case "mrfowner":
                    return 3;
                case "hr":
                    return 4;
                case "resumereviewer":
                    return 5;
                case "interviewer":
                    return 6;
                case "hiringmanager":
                    return 7;
                default:
                    // Handle the case when the role is not recognized
                    return -1; // or throw an exception, or use a different default value
            }
        }


        public string GetRole(int roleId)
        {
            switch (roleId)
            {
                case 1:
                    return "superadmin";
                case 2:
                    return "admin";
                case 3:
                    return "mrfowner";
                case 4:
                    return "hr";
                case 5:
                    return "resumereviewer";
                case 6:
                    return "interviewer";
                case 7:
                    return "hiringmanager";
                default:
                    // Handle the case when the role is not recognized
                    return ""; // or throw an exception, or use a different default value
            }
        }

    }
}
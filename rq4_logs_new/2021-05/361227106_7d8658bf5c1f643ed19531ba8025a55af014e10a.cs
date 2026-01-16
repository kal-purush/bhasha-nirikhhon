using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TAB_clinic_Data.Database;

namespace TAB_clinic_Business
{
    public static class AdminService
    {
        static AdminService()
        {

        }

        public static List<Object>  getUsers()
        {
            using (var context = new ClinicDBContext())
            {
                var source = (from user in context.Users
                               select new { id = user.IdUser, login =  user.Login, active = user.Active, role = user.Role });
                return source.ToList<Object>();
            }
        }

    }
}
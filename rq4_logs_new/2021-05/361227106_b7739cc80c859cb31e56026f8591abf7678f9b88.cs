using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TAB_clinic_Model
{
    public static class SampleDataGenerator
    {
        public static void AddSampleUsers()
        {
            try
            {
                UserContext.CreateUser("natalia", "cheba", TAB_clinic_Model.ClinicRole.Registrar);
                UserContext.CreateUser("dr", "augustyn", TAB_clinic_Model.ClinicRole.Doctor);
                UserContext.CreateUser("jerzy", "bodzenta", TAB_clinic_Model.ClinicRole.LabManager);
                UserContext.CreateUser("dariusz", "mrozek", TAB_clinic_Model.ClinicRole.LabWorker);
            }
            catch (UserAlreadyExistsException)
            {}
        }


    }
}
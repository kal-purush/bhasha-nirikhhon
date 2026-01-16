using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RAD_Project.BasicClasses
{
    class Doctor:User
    {
        public Doctor() { UserType = "Doctor"; }
        public string MedicalLicenceNo { get; set; }
        public string SpecilizationId { get; set; }
        public string Qualification { get; set; }
        public string Hospital { get; set; }
        public int YearsOfExp{ get; set;}
        public DateTime Time {  get; set; }
    }
}
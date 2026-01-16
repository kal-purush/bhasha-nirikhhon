using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AdmissionSystem2.Models
{
    public class ApplicantForUpdate
    {
        public int ID { get; set; }
        public String FirstName { set; get; }
        public String SecondName { set; get; }
        public String LastName { set; get; }
        public String PlaceOfBirth { set; get; }
        public String DateOfBirth { set; get; }
        public String Nationality { set; get; }
        public String Relegion { set; get; }
        public String Mobile { set; get; }
        public String Gender { set; get; }
        public String FamilyStatus { set; get; }
        public String SpokenLanguage { set; get; }
        public String Status { set; get; }
    }
}
using AdmissionSystem2.Entites;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AdmissionSystem2.Models
{
    public class Application
    {
        public  Applicant Applicant { get; set; }
        public IEnumerable<ParentInfo> ParentInfo { set; get; } = new List<ParentInfo>();
        public IEnumerable<EmergencyContact> EmergencyContact { set; get; } = new List<EmergencyContact>();
        public IEnumerable<Document> Documents { set; get; } = new List<Document>();
        public AdmissionDetails AdmissionDetails { set; get; }
        public IEnumerable<Sibling> Sibling { set; get; }
            = new List<Sibling>();
        public MedicalHistory MedicalHistory { set; get; }
        public Payment Payment { set; get; }
    }
}
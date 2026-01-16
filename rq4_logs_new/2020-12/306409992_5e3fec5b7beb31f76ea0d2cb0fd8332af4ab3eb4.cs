using HealthClinic.Repository;
using Model.AllActors;
using Model.DoctorMenager;
using Model.Term;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Backend.Model.PatientDoctor
{
    public class EPrescription : IIdentifiable<int>
    {
        public int Id { get; set; }
        public string Comment { get; set; }
        public string PatientName { get; set; }
        public int PatientId { get; set; }
        public string MedicamentName { get; set; }
        public DateTime PublishingDate { get; set; }
        public EPrescription() { }

        public EPrescription(int id, int patientId, string patientName,string medicamentName, string comment, DateTime publishingDate)
        {
            Id = id;
            PatientId = patientId;
            PatientName = patientName;
            MedicamentName = medicamentName;
            Comment = comment;
            PublishingDate = publishingDate;
        }

        public EPrescription(int id, string comment, DateTime publishingDate, int medicalExaminationId, List<Medicament> medicaments)
        {
            Id = id;
            Comment = comment;
            PublishingDate = publishingDate;
        }

        public int GetId()
        {
            return Id;
        }

        public void SetId(int id)
        {
            Id = id;
        }
    }
}
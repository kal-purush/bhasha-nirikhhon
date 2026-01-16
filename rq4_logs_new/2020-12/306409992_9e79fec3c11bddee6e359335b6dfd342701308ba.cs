using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace WebAppPatient.Dto
{
    public class AppointmentDto
    {
        public int Id { get; set; }
        public bool Canceled { get; set; }
        public String Date { get; set; }
        public String Period { get; set; }
        public String DoctorFullName { get; set; }
        public String DoctorSpecialization { get; set; }
        public String Room { get; set; }
        public int MedicalExaminationId { get; set; }
        public AppointmentDto() { }
    }
}
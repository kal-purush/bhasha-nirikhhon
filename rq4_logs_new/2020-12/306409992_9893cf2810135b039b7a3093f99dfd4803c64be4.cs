using Model.Term;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace WebAppPatient.Dto
{
    public class AppointmentMDto
    {
        public int Id { get; set; }
        public bool Canceled { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public MedicalExamination MedicalExamination { get; set; }
        public int DoctorWorkDayId { get; set; }

        public AppointmentMDto()
        {
        }
    }
}
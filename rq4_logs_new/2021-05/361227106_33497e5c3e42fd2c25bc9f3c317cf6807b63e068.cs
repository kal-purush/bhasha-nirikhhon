using System;
using static TAB_clinic_Model.VisitStatusMethods;

namespace TAB_clinic_Model
{
    public class VisitsRenderModel
    {

        private int idVisit;
        private DateTime dateTimeRegistered;
        private DateTime? dateTimeFinalizedCancelled;
        private string doctorName;
        private string registrarName;
        private string patientName;
        private string patientPesel;
        private VisitStatus status;

        public int IdVisit
        {
            get => this.idVisit;
            set => this.idVisit = value;
        }
        
        public DateTime DateTimeRegistered
        {
            get => this.dateTimeRegistered;
            set => this.dateTimeRegistered = value;
        }

        public DateTime? DateTimeFinalizedCancelled
        {
            get => this.dateTimeFinalizedCancelled;
            set => this.dateTimeFinalizedCancelled = value;
        }

        public string DoctorName
        {
            get => this.doctorName;
            set => this.doctorName = value;
        }
        
        public string RegistrarName
        {
            get => this.registrarName;
            set => this.registrarName = value;
        }
        
        public string PatientName
        {
            get => this.patientName;
            set => this.patientName = value;
        }
        
        public string PatientPesel
        {
            get => this.patientPesel;
            set => this.patientPesel = value;
        }
        
        public VisitStatus Status
        {
            get => this.status;
            set => this.status = value;
        }
    }
}
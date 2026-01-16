using Backend.Repository.ExaminationSurgeryRepository;
using Backend.Service.ExaminationSurgeryServices;
using Model.Term;
using Model.AllActors;
using Moq;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace WebAppPatientTests.UnitTests
{
    public class AppointmentsTests
    {
        
        [Fact]
        public void Get_all_cencelled_appointments_by_patient()
        {
            AppointmentService appointmentService = new AppointmentService(CreateDoctorWorkDayStubRepository());

            List<Appointment> result = appointmentService.getAllCancelledAppointmentsByPatient(1);

            Assert.Equal(0, result.Count);
        }

        private static IAppointmentRepository CreateDoctorWorkDayStubRepository()
        {
            var stubRepository = new Mock<IAppointmentRepository>();

            var appointments = new List<Appointment>();
            appointments.Add(new Appointment(1, false, new DateTime(2020, 12, 5, 8, 0, 0), 1, new MedicalExamination(1, false, "", 1, 1, 1, new Room(), new Model.AllActors.Doctor(), new Patient()), 1, new DoctorWorkDay()));
            appointments.Add(new Appointment(2, false, new DateTime(2020, 12, 5, 8, 30, 0), 1, new MedicalExamination(1, false, "", 1, 1, 1, new Room(), new Doctor(), new Patient()), 1, new DoctorWorkDay()));
            appointments.Add(new Appointment(3, false, new DateTime(2020, 12, 5, 10, 30, 0), 1, new MedicalExamination(1, false, "", 1, 1, 2, new Room(), new Doctor(), new Patient()), 1, new DoctorWorkDay()));
            appointments.Add(new Appointment(4, false, new DateTime(2020, 12, 7, 15, 0, 0), 1, new MedicalExamination(1, false, "", 1, 1, 2, new Room(), new Doctor(), new Patient()), 1, new DoctorWorkDay()));
            appointments.Add(new Appointment(5, false, new DateTime(2020, 12, 7, 16, 0, 0), 1, new MedicalExamination(1, false, "", 1, 1, 2, new Room(), new Doctor(), new Patient()), 1, new DoctorWorkDay()));
            appointments.Add(new Appointment(6, false, new DateTime(2020, 12, 7, 18, 0, 0), 1, new MedicalExamination(1, false, "", 1, 1, 1, new Room(), new Doctor(), new Patient()), 1, new DoctorWorkDay()));
            appointments.Add(new Appointment(7, false, new DateTime(2020, 12, 7, 12, 30, 0), 1, new MedicalExamination(1, false, "", 1, 1, 1, new Room(), new Doctor(), new Patient()), 1, new DoctorWorkDay()));

            stubRepository.Setup(appointmentsRepository => appointmentsRepository.GetAllEntities()).Returns(appointments);
            return stubRepository.Object;
        }
    }
}
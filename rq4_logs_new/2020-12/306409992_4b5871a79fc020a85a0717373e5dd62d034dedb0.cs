using Backend.Model.AllActors;
using Backend.Repository.ExaminationSurgeryRepository;
using Backend.Repository.UsersRepository;
using Backend.Service.ExaminationSurgeryServices;
using Backend.Service.UsersServices;
using Model.AllActors;
using Model.Term;
using Moq;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace HospitalMapTests.UnitTests
{
    public class AppointmentTests
    {
        [Fact]
        public void Get_all_appointments_by_patient()
        {
            AppointmentService appointmentService = new AppointmentService(CreateDoctorWorkDayStubRepository(), null);

            List<Appointment> result = appointmentService.GetAllAppointmentsByPatient(1);

            Assert.Equal(4, result.Count);
        }

        [Fact]
        public void Get_all_appointments_by_patient_invalid()
        {
            AppointmentService appointmentService = new AppointmentService(CreateDoctorWorkDayStubRepository(), null);

            List<Appointment> result = appointmentService.GetAllAppointmentsByPatient(1);

            Assert.NotEqual(2, result.Count);
        }

        [Fact]
        public void Get_previous_appointments_by_patient()
        {
            AppointmentService appointmentService = new AppointmentService(CreateDoctorWorkDayStubRepository(), null);

            List<Appointment> result = appointmentService.GetPreviousAppointmetsByPatient(1, new DateTime(2020, 12, 6));

            Assert.Equal(2, result.Count);
        }

        [Fact]
        public void Get_previous_appointments_by_patient_invalid()
        {
            AppointmentService appointmentService = new AppointmentService(CreateDoctorWorkDayStubRepository(), null);

            List<Appointment> result = appointmentService.GetPreviousAppointmetsByPatient(1, new DateTime(2020, 12, 4));

            Assert.NotEqual(2, result.Count);
        }

        [Fact]
        public void Get_scheduled_appointments_by_patient()
        {
            AppointmentService appointmentService = new AppointmentService(CreateDoctorWorkDayStubRepository(), null);

            List<Appointment> result = appointmentService.GetScheduledAppointmetsByPatient(1, new DateTime(2020, 12, 6));

            Assert.Equal(2, result.Count);
        }

        [Fact]
        public void Get_scheduled_appointments_by_patient_invalid()
        {
            AppointmentService appointmentService = new AppointmentService(CreateDoctorWorkDayStubRepository(), null);

            List<Appointment> result = appointmentService.GetScheduledAppointmetsByPatient(1, new DateTime(2020, 12, 8));

            Assert.NotEqual(2, result.Count);
        }

        [Fact]
        public void Cancel_patient_appointment()
        {
            AppointmentService appointmentService = new AppointmentService(CreateDoctorWorkDayStubRepository(), null);

            appointmentService.CancelPatientAppointment(3, new DateTime(2020,12,4));
            Appointment canceledAppointment = appointmentService.GetEntity(3);

            Assert.True(canceledAppointment.Canceled);
        }

        [Fact]
        public void Get_all_cencelled_appointments_by_patient()
        {
            AppointmentService appointmentService = new AppointmentService(CreateDoctorWorkDayStubRepository(), null);

            List<Appointment> result = appointmentService.getAllCancelledAppointmentsByPatient(2);

            Assert.Equal(1, result.Count);
        }

        [Fact]
        public void Cancel_patient_appointment_invalid_data()
        {
            AppointmentService appointmentService = new AppointmentService(CreateDoctorWorkDayStubRepository(), null);

            appointmentService.CancelPatientAppointment(2, new DateTime(2020, 12, 4));
            Appointment canceledAppointment = appointmentService.GetEntity(2);

            Assert.False(canceledAppointment.Canceled);
        }

        [Fact]
        public void Get_all_malicious_patients()
        {
            PatientService patientService = new PatientService(CreatePatientStubRepository());
            AppointmentService appointmentService = new AppointmentService(CreateDoctorWorkDayStubRepository(), patientService);

            appointmentService.CancelPatientAppointment(8, new DateTime(2020,12,4));
            appointmentService.CancelPatientAppointment(9, new DateTime(2020, 12, 4));
            appointmentService.CancelPatientAppointment(10, new DateTime(2020, 12, 4));

            List<Patient> result = patientService.GetMaliciousPatients();

            Assert.Equal(1, result.Count);
        }

        private static IAppointmentRepository CreateDoctorWorkDayStubRepository()
        {
            var stubRepository = new Mock<IAppointmentRepository>();

            var appointments = new List<Appointment>();
            appointments.Add(new Appointment(1, false, new DateTime(2020, 12, 5, 8, 0, 0), 1, new MedicalExamination(1, false, "", 1, 1, 1, new Room(), new Doctor(), new Patient()), 1, new DoctorWorkDay()));
            appointments.Add(new Appointment(2, false, new DateTime(2020, 12, 5, 8, 30, 0), 1, new MedicalExamination(1, false, "", 1, 1, 1, new Room(), new Doctor(), new Patient()), 1, new DoctorWorkDay()));
            appointments.Add(new Appointment(3, true, new DateTime(2020, 12, 5, 10, 30, 0), 1, new MedicalExamination(1, false, "", 1, 1, 2, new Room(), new Doctor(), new Patient()), 1, new DoctorWorkDay()));
            appointments.Add(new Appointment(4, false, new DateTime(2020, 12, 7, 15, 0, 0), 1, new MedicalExamination(1, false, "", 1, 1, 2, new Room(), new Doctor(), new Patient()), 1, new DoctorWorkDay()));
            appointments.Add(new Appointment(5, false, new DateTime(2020, 12, 7, 16, 0, 0), 1, new MedicalExamination(1, false, "", 1, 1, 2, new Room(), new Doctor(), new Patient()), 1, new DoctorWorkDay()));
            appointments.Add(new Appointment(6, false, new DateTime(2020, 12, 7, 18, 0, 0), 1, new MedicalExamination(1, false, "", 1, 1, 1, new Room(), new Doctor(), new Patient()), 1, new DoctorWorkDay()));
            appointments.Add(new Appointment(7, false, new DateTime(2020, 12, 7, 12, 30, 0), 1, new MedicalExamination(1, false, "", 1, 1, 1, new Room(), new Doctor(), new Patient()), 1, new DoctorWorkDay()));

            appointments.Add(new Appointment(8, false, new DateTime(2020, 12, 1, 12, 30, 0), new DateTime(2020, 12, 20, 12, 30, 0), 1, new MedicalExamination(1, false, "", 1, 1, 3, new Room(), new Doctor(), new Patient()), 1, new DoctorWorkDay()));
            appointments.Add(new Appointment(9, false, new DateTime(2020, 12, 2, 12, 30, 0), new DateTime(2020, 12, 21, 12, 30, 0), 1, new MedicalExamination(1, false, "", 1, 1, 3, new Room(), new Doctor(), new Patient()), 1, new DoctorWorkDay()));
            appointments.Add(new Appointment(10, false, new DateTime(2020, 11, 21, 13, 30, 0), new DateTime(2020, 12, 22, 12, 30, 0), 1, new MedicalExamination(1, false, "", 1, 1, 3, new Room(), new Doctor(), new Patient()), 1, new DoctorWorkDay()));

            stubRepository.Setup(appointmentsRepository => appointmentsRepository.GetAllEntities()).Returns(appointments);
            stubRepository.Setup(appointmentsRepository => appointmentsRepository.GetEntity(It.IsAny<int>())).Returns((int id) => appointments.Find(m => m.Id == id));
            stubRepository.Setup(appointment => appointment.UpdateEntity(It.IsAny<Appointment>()))
                .Callback((Appointment appointment) =>
                {
                    Appointment existingAppointment = appointments.Find(m => m.Id == appointment.Id);
                    existingAppointment.Canceled = true;
                });
            return stubRepository.Object;
        }

        private static IPatientRepository CreatePatientStubRepository()
        {
            var stubRepository = new Mock<IPatientRepository>();

            var patients = new List<Patient>();
            patients.Add(new Patient { Id = 3, Name = "Petar", Surname = "Petrovic", ParentName = "Zika", Gender = Gender.Male, IdentityCard = "123123123", HealthInsuranceCard = "32312312312", Jmbg = "13312312312312", BloodGroup = BloodGroup.AbMinus, DateOfBirth = new DateTime(2000, 1, 1, 3, 3, 3), ContactNumber = "063554533", EMail = "pera@gmail.com", Username = "pera", Password = "123", GuestAccount = false, CityId = 1, Blocked = false, Malicious = false });

            stubRepository.Setup(patientRepository => patientRepository.GetAllEntities()).Returns(patients);
            stubRepository.Setup(patientRepository => patientRepository.GetEntity(It.IsAny<int>())).Returns((int id) => patients.Find(m => m.Id == id));
            stubRepository.Setup(patient => patient.UpdateEntity(It.IsAny<Patient>()))
                .Callback((Patient patient) =>
                {
                    Patient existingPatient = patients.Find(m => m.Id == patient.Id);
                    existingPatient.Malicious = true;
                });
            return stubRepository.Object;
        }
    }
}
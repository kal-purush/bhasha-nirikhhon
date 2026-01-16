using Backend.Repository.ExaminationSurgeryRepository;
using Backend.Repository.UsersRepository;
using Backend.Service.UsersServices;
using Model.Term;
using Moq;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace WebAppPatientTests.UnitTests
{
    public class DoctorWorkDayTests
    {
        [Fact]
        public void Get_doctor_work_day_by_date_and_doctor_id()
        {
            DoctorWorkDayService doctorWorkDayService = new DoctorWorkDayService(CreateDoctorWorkDayStubRepository());

            DoctorWorkDay result = doctorWorkDayService.GetDoctorWorkDayByDateAndDoctorId(new DateTime(2020, 12, 7), 2);

            Assert.NotNull(result);
        }

        [Fact]
        public void Get_doctor_work_day_by_date_and_doctor_id_invalid()
        {
            DoctorWorkDayService doctorWorkDayService = new DoctorWorkDayService(CreateDoctorWorkDayStubRepository());

            DoctorWorkDay result = doctorWorkDayService.GetDoctorWorkDayByDateAndDoctorId(new DateTime(2020, 12, 7), 1);

            Assert.Null(result);
        }

        [Fact]
        public void Get_available_appointments_by_date_and_doctor_id()
        {
            DoctorWorkDayService doctorWorkDayService = new DoctorWorkDayService(CreateDoctorWorkDayStubRepository());

            List<Appointment> result = doctorWorkDayService.GetAvailableAppointmentsByDateAndDoctorId(new DateTime(2020, 12, 5), 1);

            Assert.Equal(21, result.Count);
        }

        [Fact]
        public void Get_available_appointments_by_date_and_doctor_id_invalid()
        {
            DoctorWorkDayService doctorWorkDayService = new DoctorWorkDayService(CreateDoctorWorkDayStubRepository());

            List<Appointment> result = doctorWorkDayService.GetAvailableAppointmentsByDateAndDoctorId(new DateTime(2020, 12, 7), 2);

            Assert.NotEqual(21, result.Count);
        }

        private static IDoctorWorkDayRepository CreateDoctorWorkDayStubRepository()
        {
            var stubRepository = new Mock<IDoctorWorkDayRepository>();

            var firstDoctorAppointments = new List<Appointment>();
            var secondDoctorAppointments = new List<Appointment>();
            var doctorWorkDays = new List<DoctorWorkDay>();
            firstDoctorAppointments.Add(new Appointment(1, false, new DateTime(2020, 12, 5, 8, 0, 0), new DateTime(2020, 12, 5, 8, 30, 0)));
            firstDoctorAppointments.Add(new Appointment(2, false, new DateTime(2020, 12, 5, 8, 30, 0), new DateTime(2020, 12, 5, 9, 0, 0)));
            firstDoctorAppointments.Add(new Appointment(3, false, new DateTime(2020, 12, 5, 10, 30, 0), new DateTime(2020, 12, 5, 11, 0, 0)));
            secondDoctorAppointments.Add(new Appointment(4, false, new DateTime(2020, 12, 7, 15, 0, 0), new DateTime(2020, 12, 5, 15, 30, 0)));
            secondDoctorAppointments.Add(new Appointment(5, false, new DateTime(2020, 12, 7, 16, 0, 0), new DateTime(2020, 12, 5, 16, 30, 0)));
            secondDoctorAppointments.Add(new Appointment(6, false, new DateTime(2020, 12, 7, 18, 0, 0), new DateTime(2020, 12, 5, 18, 30, 0)));
            secondDoctorAppointments.Add(new Appointment(7, false, new DateTime(2020, 12, 7, 12, 30, 0), new DateTime(2020, 12, 5, 13, 0, 0)));

            doctorWorkDays.Add(new DoctorWorkDay(1, new DateTime(2020, 12, 5), 1, firstDoctorAppointments));
            doctorWorkDays.Add(new DoctorWorkDay(2, new DateTime(2020, 12, 7), 2, secondDoctorAppointments));

            stubRepository.Setup(doctorWorkDayRepository => doctorWorkDayRepository.GetAllEntities()).Returns(doctorWorkDays);
            return stubRepository.Object;
        }

       
    }
}
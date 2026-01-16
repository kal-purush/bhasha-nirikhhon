using Microsoft.AspNetCore.Mvc.Testing;
using Shouldly;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using WebAppPatient;
using WebAppPatient.Dto;
using Xunit;
using Newtonsoft.Json;

namespace IntegrationTests.WebAppPatientIntegrationTests
{
    public class AppointmentTestsIntegration : IClassFixture<WebApplicationFactory<Startup>>
    {
        private readonly WebApplicationFactory<Startup> factory;

        public AppointmentTestsIntegration(WebApplicationFactory<Startup> factory)
        {
            this.factory = factory;
        }

        [Theory]
        [MemberData(nameof(AppointmentData))]
        public async void Get_Previous_Appointments_Status_Code_Test(int patientId, HttpStatusCode expectedResponseStatusCode)
        {
            HttpClient client = factory.CreateClient();

            var response = await client.GetAsync("/api/appointment/getPreviousAppointmetsByPatient/" + patientId);

            response.StatusCode.ShouldBeEquivalentTo(expectedResponseStatusCode);
        }

        [Theory]
        [MemberData(nameof(AppointmentData))]
        public async void Get_Scheduled_Appointments_Status_Code_Test(int patientId, HttpStatusCode expectedResponseStatusCode)
        {
            HttpClient client = factory.CreateClient();

            var response = await client.GetAsync("/api/appointment/getScheduledAppointmetsByPatient/" + patientId);

            response.StatusCode.ShouldBeEquivalentTo(expectedResponseStatusCode);
        }

        [Theory]
        [MemberData(nameof(CancelAppointmentData))]
        public async void Cancel_Appointment_Status_Code_Test(int appointmentId, HttpStatusCode expectedResponseStatusCode)
        {
            HttpClient client = factory.CreateClient();

            var response = await client.PutAsync("/api/appointment/cancelAppointment/" + appointmentId, new StringContent("1", Encoding.UTF8, "application/json"));

            response.StatusCode.ShouldBeEquivalentTo(expectedResponseStatusCode);
        }

        [Fact]
        public async void Find_All_Recommended_Appointments()
        {
            HttpClient client = factory.CreateClient();

            HttpResponseMessage response = await client.GetAsync("/api/appointment/getAllRecommendedTerms?startDate=2020-12-19&endDate=2020-12-20&doctorId=2&priority=Doctor");

            response.StatusCode.ShouldBeEquivalentTo(HttpStatusCode.OK);
        }

        [Fact]
        public async void Find_All_Recommended_Appointments_With_Bad_Parameters()
        {
            HttpClient client = factory.CreateClient();

            HttpResponseMessage response = await client.GetAsync("/api/appointment/getAllRecommendedTerms?startDate=2020-12-39&endDate=2020-12-20&doctorId=2&priority=Doctor");

            response.StatusCode.ShouldBeEquivalentTo(HttpStatusCode.BadRequest);
        }

        [Fact]
        public async void Find_Available_Appointments()
        {
            HttpClient client = factory.CreateClient();

            HttpResponseMessage response = await client.GetAsync("/api/appointment/getAvailableAppointments?date=2020-12-20&doctorId=1");

            response.StatusCode.ShouldBeEquivalentTo(HttpStatusCode.OK);
        }

        [Fact]
        public async void Find_Available_Appointments_With_Bad_Parameters()
        {
            HttpClient client = factory.CreateClient();

            HttpResponseMessage response = await client.GetAsync("/api/appointment/getAvailableAppointments?date=2020-12-24&doctorId=1");

            response.StatusCode.ShouldBeEquivalentTo(HttpStatusCode.NoContent);
        }

        [Theory]
        [MemberData(nameof(ScheduleAppointmentData))]
        public async void Schedule_Appointment_Not_Valid(SchedulingAppointmentDto appointment, HttpStatusCode expectedResponseStatusCode)
        {
            HttpClient client = factory.CreateClient();

            HttpResponseMessage response = await client.PostAsync("/api/appointment/", new StringContent(JsonConvert.SerializeObject(appointment), Encoding.UTF8, "application/json"));

            response.StatusCode.ShouldBeEquivalentTo(expectedResponseStatusCode);
        }

        public static IEnumerable<object[]> AppointmentData()
        {
            var retVal = new List<object[]>();
            retVal.Add(new object[] { 1, HttpStatusCode.OK });
            retVal.Add(new object[] { 50, HttpStatusCode.NotFound });
            return retVal;
        }

        public static IEnumerable<object[]> CancelAppointmentData()
        {
            var retVal = new List<object[]>();
            retVal.Add(new object[] { 15, HttpStatusCode.OK });
            retVal.Add(new object[] { 50, HttpStatusCode.NotFound });
            return retVal;
        }

        public static IEnumerable<object[]> ScheduleAppointmentData()
        {
            var retVal = new List<object[]>();
            retVal.Add(new object[] { new SchedulingAppointmentDto { Canceled = false, StartTime = new DateTime(2020, 12, 21, 19, 30, 0), EndTime = new DateTime(2020, 12, 21, 20, 0, 0), DoctorWorkDayId = 5, MedicalExamination = new Model.Term.MedicalExamination { ShortDescription = "", RoomId = 3, DoctorId = 2 } }, HttpStatusCode.NotFound });
            return retVal;
        }
    }
}
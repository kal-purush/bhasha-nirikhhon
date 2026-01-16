using AutoMapper;
using CIS.API.DataTransferObjects.Departments.Request;
using CIS.API.DataTransferObjects.Departments.Response;
using CIS.ServiceContracts.ServiceObjects;
using CIS.API.DataTransferObjects.Gender.Response;
using CIS.API.DataTransferObjects.Gender.Request;
using System;
using CIS.API.DataTransferObjects.Patients.Request;
using CIS.API.DataTransferObjects.Patients.Response;
using CIS.API.DataTransferObjects.Appointments.Request;
using CIS.API.DataTransferObjects.Appointments.Response;

namespace CIS.API.Configuration
{
    public sealed class AutoMapperProfile : Profile
    {
        public AutoMapperProfile()
        {
            this.CreateMapForGender();
            this.CreateMapForDepartments();

            this.CreateMapForPatients();
            this.CreateMapForAppointments();
        }     

        private void CreateMapForGender()
        {
            this.CreateMap<AddGenderRequestDto, GenderServiceObject>();
            this.CreateMap<GenderServiceObject, AddGenderResponseDto>();

            this.CreateMap<UpdateGenderRequestDto, GenderServiceObject>();
            this.CreateMap<GenderServiceObject, UpdateGenderResponseDto>();

            this.CreateMap<GenderServiceObject, GetGenderResponseDto>();
        }
        private void CreateMapForDepartments()
        {
            this.CreateMap<CreateDepartmentRequestDto, DepartmentServiceObject>();
            this.CreateMap<DepartmentServiceObject, CreateDepartmentResponseDto>();

            this.CreateMap<UpdateDepartmentRequestDto, DepartmentServiceObject>();
            this.CreateMap<DepartmentServiceObject, UpdateDepartmentResponseDto>();

            this.CreateMap<DepartmentServiceObject, GetDepartmentResponseDto>();
        }

        private void CreateMapForPatients()
        {
            this.CreateMap<CreatePatientRequestDto, UserServiceObject>();
            this.CreateMap<UserServiceObject, CreatePatientResponseDto>();
            this.CreateMap<UserServiceObject, GetPatientResponseDto>();
        }

        private void CreateMapForAppointments()
        {
            this.CreateMap<CreateAppointmentRequestDto, AppointmentServiceObject>();
            this.CreateMap<AppointmentServiceObject, CreateAppointmentResponseDto>();
            this.CreateMap<AppointmentServiceObject, SearchAppointmentsResponseDto>();

            this.CreateMap<AppointmentStatusServiceObject, AppointmentStatusResponseDto>();
        }
    }
}
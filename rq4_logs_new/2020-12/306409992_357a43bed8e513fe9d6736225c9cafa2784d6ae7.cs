using Model.Term;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using WebAppPatient.Dto;

namespace WebAppPatient.Mapper
{
    public class DoctorWorkDayMapper
    {

        public static DoctorWorkDayDto DoctorWorkDayToDoctorWorkDayDto(DoctorWorkDay doctorWorkDay, List<Appointment> availableAppointments)
        {
            DoctorWorkDayDto dto = new DoctorWorkDayDto();
            dto.Id = doctorWorkDay.Id;
            dto.DoctorId = doctorWorkDay.DoctorId;
            dto.RoomId = doctorWorkDay.RoomId;
            dto.AvailableAppointments = availableAppointments;

            return dto;
        }

    }
}
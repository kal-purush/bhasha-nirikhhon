using Auction.LMS.CORE.Data;
using Auction.LMS.CORE.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Auction.LMS.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class AttendanceController : ControllerBase
    {
        private readonly IAttendanceService attendanceService;
        public AttendanceController(IAttendanceService _attendanceService)
        {
            attendanceService = _attendanceService;
        }
        [HttpGet]
        [ProducesResponseType(typeof(List<Attendance>), StatusCodes.Status200OK)]
        public List<Attendance> GetAllAttendance()
        {
            return attendanceService.GetAllAttendance();
        }
        [HttpPost]
        public bool InsertAttendance([FromBody] Attendance attendance)
        {
            return attendanceService.InsertAttendance(attendance);
        }
        [HttpPut]
        public bool UpdateAttendance(Attendance attendance)
        {
            return attendanceService.UpdateAttendance(attendance);
        }
        [HttpDelete("delete/{id}")]
        public bool DeleteAttendance(int Attendanceid)
        {
            return attendanceService.DeleteAttendance(Attendanceid);
        }
        [HttpGet]
        [Route("getAttendancebyid/{id}")]
        public Attendance GetAllAttendanceId(int id)
        {
            return attendanceService.GetAllAttendanceId(id);
        }

    }
}
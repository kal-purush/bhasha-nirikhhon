using geesRecorder.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace geesRecorder.Controllers
{
    [Authorize]
    public class AttendanceController : ControllerBase
    {
        private readonly IAttendanceManager _attendanceManager;

        public AttendanceController(IAttendanceManager attendanceManager)
        {
            _attendanceManager = attendanceManager;
        }

        [HttpGet("getattendance")]
        public async Task<IActionResult> GetAttendance(string attendanceName)
        {
            throw new NotImplementedException();
        }
    }
}
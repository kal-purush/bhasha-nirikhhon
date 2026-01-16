using geesRecorder.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace geesRecorder.Interfaces
{
    public interface IAttendanceManager
    {
        Task CreateAttendanceAsync();

        Task MarkAttendanceAsync(string attendantId, bool attended);

        Task<Attendance> GetAttendanceAsync(string attendanceName);

        Task OpenAttendanceAsync(int timeInMinutes);

        Task CloseAttendanceAsync();
    }
}
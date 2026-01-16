using CRM.Models;
using CRM.UoW;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CRM.Services
{
    public class TimeTableService
    {
        private readonly UnitOfWork _unitOfWork;

        public TimeTableService(UnitOfWork unitOfWork)
        {
            _unitOfWork = unitOfWork;
        }

        public async Task<List<TimeTable>> GetAllTimeTable()
        {
            var timeTablesUoF = _unitOfWork.TimeTables;
            var timeTables = await timeTablesUoF.GetAllAsync();

            return timeTables;
        }

        public async Task<TimeTable> GetTimeTableById(int id)
        {
            var timeTablesUoF = _unitOfWork.TimeTables;
            var timeTable = await timeTablesUoF.GetByIdAsync(id);
            return timeTable;
        }

        public async Task CreateTimeTable(TimeTable timeTable)
        {
            var timeTablesUoF = _unitOfWork.TimeTables;
            await timeTablesUoF.CreateAsync(timeTable);
            await _unitOfWork.CompleteAsync();
        }

        public async Task EditTimeTable(TimeTable timeTable)
        {
            var timeTablesUoF = _unitOfWork.TimeTables;
            timeTablesUoF.UpdateAsync(timeTable);
            await _unitOfWork.CompleteAsync();
        }

        public async Task DeleteTimeTable(TimeTable timeTable)
        {
            var timeTablesUoF = _unitOfWork.TimeTables;
            timeTablesUoF.RemoveAsync(timeTable);
            await _unitOfWork.CompleteAsync();
        }
    }
}

using PipeBandGames.DataLayer.Entities;
using System;
using System.Collections.Generic;

namespace PipeBandGames.BusinessLayer.Interfaces
{
    public interface IScheduleService
    {
        List<SoloEvent> GetSoloEventSchedule(Contest contest);
    }
}
using AutoMapper;
using DiaryApp.Core.Models;
using DiaryApp.Data.DTO;
using DiaryApp.Data.ServiceInterfaces;
using Microsoft.Extensions.Logging;

namespace DiaryApp.API.Controllers.ListItems
{
    public class HabitDaysController : CrudController<HabitDayDto, HabitDay>
    {

        public HabitDaysController(ICrudService<HabitDayDto, HabitDay> habitDayService
            , IMapper mapper, ILoggerFactory loggerFactory) : base(habitDayService, mapper, loggerFactory)
        {
        }
    }
}
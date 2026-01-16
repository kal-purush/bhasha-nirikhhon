using System;
using System.ComponentModel.DataAnnotations;
using ScheduleSystem.ScheduleAPI.BusinessLogic.Entities;

namespace ScheduleSystem.ScheduleAPI.Controllers.ModelView
{
    public class ScheduleModel
    {
        [Required]
        public Discipline Discipline { get; set; }
        [Required]
        public TypeOfOccupation TypeOfOccupation { get; set; }
        [Required]
        public Teacher Teacher { get; set; }
        [Required]
        public LectureHall LectureHall { get; set; }
        [Required]
        public DateTime Date { get; set; }
    }
}
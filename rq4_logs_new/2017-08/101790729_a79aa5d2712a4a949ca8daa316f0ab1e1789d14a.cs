using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace workforce_part_2.Models.ViewModels
{
    public class TrainingProgramDetailViewModel
    {
        public TrainingProgram TrainingProgram { get; set; }
        public List<Employee> Employee { get; set; } = new List<Employee>();
    }
}
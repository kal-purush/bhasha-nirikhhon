using Domain.Common.Entities;
using System.ComponentModel.DataAnnotations.Schema;
using Domain.Enum;

namespace Domain.Entities
{
    public class StudentModule:BaseEntity
    {
      
        [ForeignKey(nameof(StudentId))]
        public string StudentId { get; set; }

        [ForeignKey(nameof(ModuleId))]
        public int ModuleId { get; set; }

        public DateTime EnrollmentDate { get; set; } = DateTime.UtcNow;
        
        public DateTime? EndDate { get; set; }

        public StudentModuleProgress StudentModuleProgress { get; set; } = StudentModuleProgress.Pending;
        
        public string StudentModuleProgressDes { get; set; }

    }
}
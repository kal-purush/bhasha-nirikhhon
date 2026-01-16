using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraduateProject.Common.DTOs.Department
{
    public class DepartmentDTO
    {
        public int Id { get; set; }
        public virtual ICollection<DepartmentTranslateDTO> Translates { get; set; }
    }

    public class DepartmentTranslateDTO
    {
        public int DepratmentId { get; set; }

        public string LanguageId { get; set; }

        [Required]
        [StringLength(100)]
        public string Name { get; set; }
    }
}
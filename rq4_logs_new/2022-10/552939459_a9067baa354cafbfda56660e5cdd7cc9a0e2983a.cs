using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vakansion.Core;

namespace Vacancy.Repository.Dto.UnemployedDto
{
    public class UnemployedReadDto
    {
        public int ClassId { get; set; }
        public string? ClassName { get; set; }
        public int DataId { get; set; }
        public Data? Datas { get; set; }
        public int EducationId { get; set; }
        public Education? Educations { get; set; }
        public virtual ICollection<Vacancys>? Vacancies { get; set; }
    }
}
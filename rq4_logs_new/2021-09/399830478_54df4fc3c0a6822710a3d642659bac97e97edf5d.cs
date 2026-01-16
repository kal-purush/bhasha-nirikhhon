using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace NIBM_Job_Portal.Models
{
    public class AdminCompanyViewModel
    {
        [Key]
        public int Id { get; set; }
        public string Company_Name { get; set; }

        public int IndustryId { get; set; }
        public List<Industry> Industry { get; set; }
        public string Email { get; set; }
        public string DefaultPasssword { get; set; }

    }
}
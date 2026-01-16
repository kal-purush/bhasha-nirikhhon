using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace XebecPortal.Client.JobPortalTestEnv.Models
{
    public class JobSampleTest
    {
        public int Id { get; set; }

        [Required]
        public string JobTitle { get; set; }

        [Required]
        public string Location { get; set; }

        [Required]
        public string JobType { get; set; }

        [Required]
        public string Department { get; set; }

        [Required]
        public string Description { get; set; }

        [Required]
        public string Compensation { get; set; }


    }
}
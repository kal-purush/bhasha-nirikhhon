using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace NIBM_Job_Portal.Models
{
    public class StudentJobPost
    {
        [Key]
        public int Id { get; set; }

        public string Name { get; set; }

        public string Email { get; set; }

        public string Date { get; set; }
        public string Ql1 { get; set; }
        public string Ql2 { get; set; }
        public string Ql3 { get; set; }
        public string Ql4 { get; set; }

        public string Sk1 { get; set; }
        public string Sk2 { get; set; }
        public string Sk3 { get; set; }
        public string Sk4{ get; set; }
        public string Sk5 { get; set; }
        public string Sk6 { get; set; }
        public string Sk7 { get; set; }
        public string Sk8 { get; set; }
        public string Sk9 { get; set; }
        public string Sk10 { get; set; }
        public string Description { get; set; }

        public int Age { get; set; }
        public string CV { get; set; }
        public int JobId { get; set; }
        public Job Job { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace BLL.DataTransfer
{
    public class OrganizationForm
    {
        public int Id { get; set; }
        [Required]
        public string Name { get; set; }
        [Required]
        public string Address { get; set; }
        [Required]
        public string Work { get; set; }
        public List<int> EmployeeIds { get; set; }
        public string position { get; set; }

    }
}
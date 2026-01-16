using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Web;

namespace ZoolandiaRazor.Models
{
    public class Habitat
    {
        [Key]
        public int HabitatId { get; set; }
        [Required]
        public string Name { get; set; }
        [Required]
        public string Type { get; set; }
        [Required]
        public bool CurrentlyOpen { get; set; }

        public List<Animal> AnimalList { get; set; }

        public List<Employee> EmployeeList { get; set; }
    }
}
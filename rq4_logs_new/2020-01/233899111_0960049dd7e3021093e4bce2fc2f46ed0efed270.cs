using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace Bangazon_RedJags.Models.ViewModels
{
    public class TrainingWithEmployees
    {
        [Required]
        public int Id { get; set; }

        [Required(ErrorMessage = "Training Program Name is required")]
        [Display(Name = "Training Name")]
        [StringLength(255, MinimumLength = 1, ErrorMessage = "Training Program Name length should be between 1 and 255 characters")]
        public string Name { get; set; }

        [Display(Name = "Start Date")]
        [Required(ErrorMessage = "Start Date of Training Program is required")]
        [DataType(DataType.DateTime)]
        public DateTime StartDate { get; set; }

        [Required(ErrorMessage = "End Date of Training Program is required")]
        [DataType(DataType.DateTime)]
        [Display(Name = "End Date")]
        public DateTime EndDate { get; set; }

        [Display(Name = "Max Attendees")]
        [Required(ErrorMessage = "Maximum Number of Attendees is required for Training Program")]
        public int MaxAttendees { get; set; }

        public List<BasicEmployee> EmployeesAttending { get; set; } = new List<BasicEmployee>();

        public BasicEmployee BasicEmployee { get; set; }
    }
}
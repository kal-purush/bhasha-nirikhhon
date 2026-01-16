using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ZenithSocietyA2.Models
{
    public class ActivityCategory
    {
        [Display(Name =  "Activity")]
        public int ActivityCategoryId { get; set; }
        [Required]
        [Display(Name = "Activity Description")]
        public string ActivityDescription { get; set; }

        [Required]
        [Display(Name = "Creation Date")]
        //[HiddenInput(DisplayValue = true)]
        public DateTime CreationDate { get; set; }
    }
}
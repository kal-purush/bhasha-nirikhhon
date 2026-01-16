using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace TRPR.Models
{
    public class Title
    {
        public Title()
        {
            this.Researchers = new HashSet<Researcher>();
        }

        public int ID { get; set; }

        [Display(Name = "Title")]
        [Required(ErrorMessage = "You cannot leave the title blank.")]
        [StringLength(10, ErrorMessage = "Title must be under 10 characters")]
        public string Name { get; set; }

        public ICollection<Researcher> Researchers { get; set; }
    }
}
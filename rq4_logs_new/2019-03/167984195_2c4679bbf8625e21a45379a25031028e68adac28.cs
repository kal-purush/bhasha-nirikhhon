using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace TRPR.Models
{
    public class PaperType
    {
        public PaperType()
        {
            this.PaperInfos = new HashSet<PaperInfo>();
        }

        public int ID { get; set; }

        [Display(Name = "Type of Paper")]
        [Required(ErrorMessage = "You cannot leave the type blank.")]
        [StringLength(100, ErrorMessage = "Name must be under 100 characters")]
        public string Name { get; set; }

        public ICollection<PaperInfo> PaperInfos { get; set; }
    }
}

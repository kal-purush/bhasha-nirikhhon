using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DTO
{
    public class Service: BaseDTO
    {
        [Required(ErrorMessage = "Name is required")]
        public string Name { get; set; }

        [Required(ErrorMessage = "Description is required")]
        public string Description { get; set; }

        [Required(ErrorMessage = "PhoneNumber is required")]
        public int Price { get; set; }

        [Required(ErrorMessage = "Branch ID is required")]
        public int BranchID { get; set; }
    }
}
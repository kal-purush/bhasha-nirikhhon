using System.ComponentModel.DataAnnotations;

namespace SalesWebMVC.Models
{
    public class NoSales
    {
        [Display(Name = "Name")]
        public string NameSeller { get; set; }
        [Display(Name = "E-mail")]
        public string Email { get; set; }
        [Display(Name = "Name")]
        public string Name { get; set; }

    }
}
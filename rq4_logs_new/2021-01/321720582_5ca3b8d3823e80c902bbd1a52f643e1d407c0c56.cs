using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace ehpad.ORM
{
    public class Brand
    {
        public int Id { get; set; }


        [Display(Name = "Marque")]
        [MaxLength(50)]
        [Required(ErrorMessage = "Vous devez ajouter une marque.")]
        [RegularExpression(@"^[\da-zA-Zàáâãäåçèéêëìíîïðòóôõöùúûüýÿ0\s-']{1,50}$",
         ErrorMessage = "Le marque est incorrecte.")]
        public String Name { get; set; }

        public List<Vaccine> Vaccinnes { get; } = new List<Vaccine>();
    }
}
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data.SqlTypes;

namespace DataAcces.Model
{
    public class Person
    {
        public int Id { get; set; }


        [Required(ErrorMessage = "Vyplnte krestni jmeno!")]
        public string FirstName { get; set; }
        [Required(ErrorMessage = "Vyplnte prijmeni!")]
        public string LastName { get; set; }
        [Required(ErrorMessage = "Vyplnte emailovou adresu")]
        public string Email { get; set; }
        [Required(ErrorMessage = "Vyplnte datum narozeni!")]
        [Range(1925,2005, ErrorMessage = "Zamestnanec je moc mlady, nebo je uz mrtvy!")]
        public int Birth { get; set; }
        public SqlDateTime Registred { get; set; }
    }
}
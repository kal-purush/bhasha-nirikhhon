using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace Darkec.Models
{
    public class Boats
    {
        [Range(1,100)]
        public int Id { get; set; }
        [StringLength(2)] //assuming that it is a 2digit number
        [Range(0,20)]
        [Required]
        public string SeatingCapacity { get; set; }
        public enum LicenseCategory { A,B,C,M}
        [Range(0,200)]
        [StringLength(6, MinimumLength = 4)] //minimum length is 4, assuming 0kmh. maximum length is 6 assuming 200kmh = 6characters
        public string TopSpeed { get; set; }
        [Range(0,10)]
        public double Length { get; set; }
        [Range(0,10000)]
        public int Weight { get; set; }
        [Required]
        [StringLength(6, MinimumLength = 3)] //based on that registration numbers have at least 1 digit or maximum 4digits and 2 letters
        public string RegistrationNumber { get; set; }
        [Range(0,1000)]
        public int EngineHorsepower { get; set; }
        [Range(0,1000000)]
        [DataType(DataType.Currency)]
        public decimal Price { get; set; }
        public string ImageName { get; set; }
        public bool Availability { get; set; }

    }
}
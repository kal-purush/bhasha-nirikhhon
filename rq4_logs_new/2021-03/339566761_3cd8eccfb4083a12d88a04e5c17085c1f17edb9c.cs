using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Boats_4_U.Models.DriverRatingModels
{
    public class DriverRatingCreate
    {
        [Required]
        public int DriverId { get; set; }

        [Required]
        public double DriverFunScore { get; set; }

        [Required]
        public double DriverSafetyScore { get; set; }

        [Required]
        public double DriverCleanlinessScore { get; set; }
    }
}
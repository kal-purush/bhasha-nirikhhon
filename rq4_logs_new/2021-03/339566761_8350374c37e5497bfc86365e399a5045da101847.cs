using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Boats_4_U.Data
{
    public class RenterRating
    {
        [Key]
        public int RenterRatingId { get; set; }

        [Required]
        public Guid ApplicationUser { get; set; }

        /// <summary>
        /// Relates this Renter to the RenterId
        /// </summary>
        [ForeignKey(nameof(Renter))]
        public int RenterId { get; set; }
        public virtual Renter Renter { get; set; }

        /// <summary>
        /// Limits the rating from 0-10
        /// </summary>
        [Required]
        [Range(0, 10)]
        public double RenterCleanlinessScore { get; set; }

        /// <summary>
        /// Limits the rating from 0-10
        /// </summary>
        [Required]
        [Range(0, 10)]
        public double RenterSafetyScore { get; set; }

        /// <summary>
        /// Limits the rating from 0-10
        /// </summary>
        [Required]
        [Range(0, 10)]
        public double RenterPunctualityScore { get; set; }

        /// <summary>
        /// Averages the 3 ratings
        /// </summary>
        public double AverageRenterRating { get
            {
                return Math.Round((RenterCleanlinessScore + RenterPunctualityScore + RenterSafetyScore) / 3, 2);   
            } 
        }


    }
}
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

        [ForeignKey(nameof(Renter))]
        public int RenterId { get; set; }
        public virtual Renter Renter { get; set; }

        [Required, Range(0, 10)]
        public double RenterFun { get; set; }

        [Required, Range(0, 10)]
        public double RenterSafety { get; set; }

        [Required, Range(0, 10)]
        public double RenterCleanliness { get; set; }

        public double AverageRenterRating
        {
            get
            {
                var totalScore = RenterFun + RenterSafety + RenterCleanliness;
                return Math.Round(totalScore / 3, 2);
            }
        }

    }
}
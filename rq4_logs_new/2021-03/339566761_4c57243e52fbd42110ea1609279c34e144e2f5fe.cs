using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Boats_4_U.Data;
using Newtonsoft.Json;

namespace Boats_4_U.Models.Renter
{
    [JsonObject(MemberSerialization.OptIn)]
    public class RenterDetailTwo
    {
        [JsonProperty]
        public int RenterId { get; set; }

        [JsonProperty]
        public Guid ApplicationUser { get; set; }

        public string RenterFirstName { get; set; }
        public string RenterLastName { get; set; }

        [JsonProperty]
        public string RenterFullName
        {
            get
            {
                var fullName = $"{RenterFirstName} {RenterLastName}";
                return fullName;
            }
        }

        [JsonProperty]
        public int RenterAge { get; set; }

        public virtual List<RenterRating> RenterRatings { get; set; }

        public double CleanlinessRating
        {
            get
            {
                double averageCleanlinessRating = 0;

                foreach (RenterRating renterRating in RenterRatings)
                {
                    averageCleanlinessRating += renterRating.RenterCleanlinessScore;
                }

                return RenterRatings.Count > 0
                    ? Math.Round(averageCleanlinessRating / RenterRatings.Count, 2)
                    : 0;
            }
        }
        public double SafetyRating
        {
            get
            {
                double averageSafetyRating = 0;

                foreach (RenterRating renterRating in RenterRatings)
                {
                    averageSafetyRating += renterRating.RenterSafetyScore;
                }

                return RenterRatings.Count > 0
                    ? Math.Round(averageSafetyRating / RenterRatings.Count, 2)
                    : 0;
            }
        }
        public double PunctualityRating
        {
            get
            {
                double averagePunctualityRating = 0;

                foreach (var renterRating in RenterRatings)
                {
                    averagePunctualityRating += renterRating.RenterPunctualityScore;
                }

                return RenterRatings.Count > 0
                    ? Math.Round(averagePunctualityRating / RenterRatings.Count, 2)
                    : 0;
            }
        }

        [JsonProperty]
        public double Rating
        {
            get
            {
                double totalAverageRating = 0;

                foreach (var rating in RenterRatings)
                {
                    totalAverageRating += rating.AverageRenterRating;
                }

                return RenterRatings.Count > 0
                    ? Math.Round(totalAverageRating / RenterRatings.Count, 2)
                    : 0;
            }
        }

        [JsonProperty]
        public bool RenterIsRecommended
        {
            get
            {
                return Rating > 8;
            }
        }
    }
}
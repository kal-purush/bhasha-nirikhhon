using CM.Web.Providers.Contracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CM.Web.Providers
{
    public static class RatingParser
    {
        public static double? ParseRating(string rating)
        {
            if (string.IsNullOrEmpty(rating))
            {
                return null;
            }
            else if (double.TryParse(rating, out double result))
            {
                return result;
            }
            else
            {
                throw new ArgumentException("Valid fraction number expected.");
            }
        }
    }
}
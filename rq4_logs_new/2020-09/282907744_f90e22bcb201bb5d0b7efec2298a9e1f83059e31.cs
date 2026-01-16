using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Billing.API.Models
{
    /// <summary>
    /// Generic class for paginated results
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class PaginatedResult<T>
    {
        /// <summary>
        /// List of paginated items
        /// </summary>
        public List<T>? Items { get; set; }

        /// <summary>
        /// Total of items
        /// </summary>
        public int TotalItems { get; set; }
    }
}
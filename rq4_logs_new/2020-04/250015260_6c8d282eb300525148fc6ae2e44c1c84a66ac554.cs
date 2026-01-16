using System.ComponentModel.DataAnnotations;

namespace Lykke.Service.Vouchers.Client.Models
{
    /// <summary>
    /// Hold information about the Current page and the amount of items on each page
    /// </summary>
    public class PaginationModel
    {
        /// <summary>
        /// The Current Page
        /// </summary>
        [Range(1, 10000)]
        public int CurrentPage { get; set; }

        /// <summary>
        /// The amount of items that the page holds
        /// </summary>
        [Range(1, 1000)]
        public int PageSize { get; set; }
    }
}
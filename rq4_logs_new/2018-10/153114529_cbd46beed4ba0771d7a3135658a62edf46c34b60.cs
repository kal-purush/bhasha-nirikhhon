using Microsoft.AspNetCore.Mvc.Rendering;
using System.Collections.Generic;

namespace Bangazon.Models.OrderViewModels
{
    public class OrderCompleteViewModel
    {
        public Order Order { get; set; }

        public double? Total { get; set; }

        public List<SelectListItem> PaymentTypes { get; set; }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PunkHouseReal.Models.BindingModels
{
    public class HouseMateExpenseFilterBindingModel
    {
        public string CreatorId { get; set; }
        public bool? IsMarkedPaid { get; set; }
        public bool? IsPaid { get; set; }
    }
}
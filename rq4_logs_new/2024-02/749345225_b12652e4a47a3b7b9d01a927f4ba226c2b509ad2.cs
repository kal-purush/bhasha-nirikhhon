using Lesson11.Models;

namespace Lesson11.ViewModels
{
    public class SupplyViewModel
    {
        public Supply Supply { get; set; }
        public SupplyItem SupplyItem { get; set; }
        public List<SupplyItem> SupplyItems { get; set; } = new List<SupplyItem>();
    }
}
using System;

namespace fix_it_tracker_back_end.Model.BindingTargets
{
    public class RepairData
    {
        public DateTime DateOpened { get; } = DateTime.UtcNow;

        public int Customer { get; set; }

        public Repair Repair => new Repair
        {
            DateOpened = DateOpened,
            DateCompleted = null,
            Customer = Customer == 0 ? null : new Customer
            {
                CustomerID = Customer,
                Name = "Name"
            }
        };
    }
}
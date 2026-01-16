using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace fix_it_tracker_back_end.Model.BindingTargets
{
    public class RepairPatchData
    {
        public DateTime? DateCompleted
        {
            get => Repair.DateCompleted; set => Repair.DateCompleted = value;
        }

        public Repair Repair { get; set; } = new Repair();
    }
}
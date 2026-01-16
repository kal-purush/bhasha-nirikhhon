using Services.Service;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Recipe_Organizer_PRN211.Plan
{
    public static class AppContext
    {
        public static PlanItem planItem { get; set; }
        public static PlanData planData { get; set; }
    }
}
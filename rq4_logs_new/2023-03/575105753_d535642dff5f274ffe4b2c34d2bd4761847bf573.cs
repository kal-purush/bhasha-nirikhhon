using ManPowerCore.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ManPowerCore.Domain
{
    [Serializable]
    public class ProgramPlanApprovalDetails
    {
        [DBField("Id")]
        public int ProgramPlanApprovalDetailsId { get; set; }

        [DBField("ProgramPlan_Id")]
        public int ProgramPlanId { get; set; }

        [DBField("ProgramPlan_Status")]
        public int ProjectStatus { get; set; }

        [DBField("Recommendation1_By")]
        public int Recommendation1By { get; set; }

        [DBField("Recommendation1_Date")]
        public DateTime Recommendation1Date { get; set; }

        [DBField("Recommendation2_By")]
        public int Recommendation2By { get; set; }

        [DBField("Recommendation2_Date")]
        public DateTime Recommendation2Date { get; set; }

        [DBField("Reject_Reason")]
        public string RejectReason { get; set; }
    }
}
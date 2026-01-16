using System.Collections.Generic;
using System.Linq;
using System.Web;
using LtInfo.Common;
using ProjectFirma.Web.Security;
using ProjectFirmaModels.Models;

namespace ProjectFirma.Web.Models
{
    public static class ObligationNumberModelExtensions
    {
        public static List<WbsElementObligationItemBudget> GetWbsElementObligationItemBudgets(this ObligationNumber obligationNumber)
        {
            return obligationNumber.ObligationItems.SelectMany(x => x.WbsElementObligationItemBudgets).OrderBy(x => x.PostingDatePerSplKey).ToList();
        }
    }
}
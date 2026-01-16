using LtInfo.Common;
using ProjectFirma.Web.Common;
using ProjectFirma.Web.Controllers;
using ProjectFirmaModels.Models;

namespace ProjectFirma.Web.Models
{
    public static class CostAuthorityObligationRequestPotentialObligationNumberMatchModelExtensions
    {
        
        public static readonly UrlTemplate<int> PotentialMatchDetailUrlTemplate = new UrlTemplate<int>(SitkaRoute<ObligationRequestController>.BuildUrlFromExpression(orc => orc.PotentialMatchDetail(UrlTemplate.Parameter1Int)));
        public static string GetPotentialMatchDetailUrl(this CostAuthorityObligationRequestPotentialObligationNumberMatch costAuthorityObligationRequestPotentialMatch)
        {
            return PotentialMatchDetailUrlTemplate.ParameterReplace(costAuthorityObligationRequestPotentialMatch.CostAuthorityObligationRequestPotentialObligationNumberMatchID);
        }


    }
}
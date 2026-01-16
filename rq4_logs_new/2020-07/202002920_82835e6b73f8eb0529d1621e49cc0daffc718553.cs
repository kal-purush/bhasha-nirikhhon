namespace ProjectFirmaModels.Models
{
    public partial class CostAuthorityObligationRequestPotentialObligationNumberMatch : IAuditableEntity
    {
        public string GetAuditDescriptionString()
        {
            var auditDescriptionString = $"CostAuthorityObligationRequestPotentialObligationNumberMatchID: {this.CostAuthorityObligationRequestPotentialObligationNumberMatchID} - CostAuthorityObligationRequestID: {this.CostAuthorityObligationRequestID} - ObligationNumberID: {this.ObligationNumberID}: (\"{this.ObligationNumber?.ObligationNumberKey}\")";
            return auditDescriptionString;
        }
    }
}
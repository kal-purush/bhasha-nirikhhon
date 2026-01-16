using ProjectFirmaModels.Models;

namespace ProjectFirma.Web.Views.ObligationRequest
{
    public class ConfirmObligationRequestUnmatchViewData : FirmaViewData
    {
        private ProjectFirmaModels.Models.ObligationRequest ObligationRequest { get; }

        public ConfirmObligationRequestUnmatchViewData(FirmaSession currentFirmaSession,
                                                       ProjectFirmaModels.Models.ObligationRequest obligationRequest) : base(currentFirmaSession)
        {
            this.ObligationRequest = obligationRequest;
        }
    }
}
using System;
using System.Collections.Generic;


namespace AltaPay.Service
{
	public class AgreementConfig
	{
        public AgreementType? AgreementType { get; set; }
        public string AgreementId { get; set; }
        public AgreementUnscheduledType? AgreementUnscheduledType { get; set; }
        public string AgreementExpiry { get; set; }
        public string AgreementFrequency { get; set; }
        public string AgreementNextChargeDate { get; set; }
        public string AgreementAdminUrl { get; set; }
		
		public Dictionary<string,Object> ToDictionary()
		{
			Dictionary<string,Object> agreementParams = new Dictionary<string,Object>();
            agreementParams.Add("id", AgreementId);
			agreementParams.Add("type", AgreementType);
			agreementParams.Add("unscheduled", AgreementUnscheduledType);
			agreementParams.Add("expiry", AgreementExpiry);
			agreementParams.Add("frequency", AgreementFrequency);
			agreementParams.Add("next_charge_date", AgreementNextChargeDate);
			agreementParams.Add("admin_url", AgreementAdminUrl);
			
			return agreementParams;
		}
	}
}

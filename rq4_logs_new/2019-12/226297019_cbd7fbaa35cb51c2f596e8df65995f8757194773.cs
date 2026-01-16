using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CIS.API.DataTransferObjects.Companies.Request
{
    public sealed class UpdateCompanyRequestDto
    {
        public string CompanyNameEnglish { get; set; }

        public string CompanyNameArabic { get; set; }

        public string CompanyAddessEnglish { get; set; }

        public string CompanyAddressArabic { get; set; }

        public string CompanyCode { get; set; }

        public string TaxId { get; set; }

        public string Currency { get; set; }

        public string Language { get; set; }

        public string LicenseStartDate { get; set; }

        public string LicenseEndDate { get; set; }

        public string HospitalId { get; set; }

        public string BankAccountNumber { get; set; }

        public string TaxSetup { get; set; }
    }
}
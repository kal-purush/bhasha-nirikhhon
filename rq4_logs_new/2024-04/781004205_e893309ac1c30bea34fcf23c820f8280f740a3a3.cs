using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DoAnCuoiKy.Class
{
    public class Employer
    {
        private int companyID;
        private string companyEmail;
        private string companyPassword;
        private string companyName;
        private string companyType;
        private string companyAddress;
        private string comapnyOverview;
        private string companyBenefit;
        private string companyWorkingDays;
        private string companySize;
        private string companyCountry;

        public int CompanyID { get => companyID; set => companyID = value; }
        public string CompanyEmail { get => companyEmail; set => companyEmail = value; }
        public string CompanyPassword { get => companyPassword; set => companyPassword = value; }
        public string CompanyName { get => companyName; set => companyName = value; }
        public string CompanyType { get => companyType; set => companyType = value; }
        public string CompanyAddress { get => companyAddress; set => companyAddress = value; }
        public string ComapnyOverview { get => comapnyOverview; set => comapnyOverview = value; }
        public string CompanyBenefit { get => companyBenefit; set => companyBenefit = value; }
        public string CompanyWorkingDays { get => companyWorkingDays; set => companyWorkingDays = value; }
        public string CompanySize { get => companySize; set => companySize = value; }
        public string CompanyCountry { get => companyCountry; set => companyCountry = value; }

        public Employer(int companyID, string companyEmail, string companyPassword, string companyName, string companyType, string companyAddress, string comapnyOverview, string companyBenefit, string companyWorkingDays, string companySize, string companyCountry)
        {
            this.companyID = companyID;
            this.companyEmail = companyEmail;
            this.companyPassword = companyPassword;
            this.companyName = companyName;
            this.companyType = companyType;
            this.companyAddress = companyAddress;
            this.comapnyOverview = comapnyOverview;
            this.companyBenefit = companyBenefit;
            this.companyWorkingDays = companyWorkingDays;
            this.companySize = companySize;
            this.companyCountry = companyCountry;
        }
    }
}
using System;
using System.Net;
using Machine.Specifications;
using S2p.RestClient.Sdk.Entities;
using S2p.RestClient.Sdk.Infrastructure;

namespace S2p.RestClient.Sdk.IntegrationTests.Mspec.Services.WebsiteManagementService
{
    public partial class When_running_a_complete_website_management_flow
    {
        private static ApiResult<ApiWebsiteManagementResponse> CreateApiResult;
        private static readonly Lazy<ApiWebsiteManagementRequest> CreateRequest = 
            new Lazy<ApiWebsiteManagementRequest>(() =>  new MerchantSite
            {
                URL = "http://www.test.com",
                Active = true,
                NotificationURL = "http://www.test.com/notifications.node",
                Alias = "Test website_sdk",
                Details = new MerchantSiteDetails
                {
                    Name = "Stichting Smart2Pay SDK",
                    Country = "PL",
                    City = "Warsaw",
                    Email = "test@test.com",
                    Address = "BRINK 27C",
                    BankName = "ING",
                    AccountIBAN = "PL641050008610000023537933350",
                    AccountSWIFT = "1000002353794650",
                    BankSWIFTID = "ABNDDEFFXXX",
                    BankCode = "BTRLRO22",
                    VATNumber = "34206701",
                    RegistrationNumber = "NL 813236460B01",
                    MCC = "7995",
                    MainBusiness = "gaming"
                }
            }.ToApiWebsiteManagementRequest());
        
        
        private const string DescriptionText = "SDK Test Website Management";

        private It create_should_have_created_http_status = () => {
            CreateApiResult.HttpResponse.StatusCode.ShouldEqual(HttpStatusCode.Created);
        };

        private It create_should_have_the_correct_url = () => {
            CreateApiResult.Value.MerchantSite.URL.ShouldEqual(CreateRequest.Value.MerchantSite.URL);
        };

        private It create_should_have_the_correct_notification_url = () => {
            CreateApiResult.Value.MerchantSite.NotificationURL.ShouldEqual(CreateRequest.Value.MerchantSite.NotificationURL);
        };

        private It create_should_have_the_correct_alias = () => {
            CreateApiResult.Value.MerchantSite.Alias.ShouldEqual(CreateRequest.Value.MerchantSite.Alias);
        };

        private It create_should_have_the_correct_name = () => {
            CreateApiResult.Value.MerchantSite.Details.Name.ShouldEqual(CreateRequest.Value.MerchantSite.Details.Name);
        };

        private It create_should_have_the_correct_country = () => {
            CreateApiResult.Value.MerchantSite.Details.Country.ShouldEqual(CreateRequest.Value.MerchantSite.Details.Country);
        };

        private It create_should_have_the_correct_city = () => {
            CreateApiResult.Value.MerchantSite.Details.City.ShouldEqual(CreateRequest.Value.MerchantSite.Details.City);
        };

        private It create_should_have_the_correct_email = () => {
            CreateApiResult.Value.MerchantSite.Details.Email.ShouldEqual(CreateRequest.Value.MerchantSite.Details.Email);
        };

        private It create_should_have_the_correct_address = () => {
            CreateApiResult.Value.MerchantSite.Details.Address.ShouldEqual(CreateRequest.Value.MerchantSite.Details.Address);
        };

        private It create_should_have_the_correct_bank_name = () => {
            CreateApiResult.Value.MerchantSite.Details.BankName.ShouldEqual(CreateRequest.Value.MerchantSite.Details.BankName);
        };

        private It create_should_have_the_correct_account_iban = () => {
            CreateApiResult.Value.MerchantSite.Details.AccountIBAN.ShouldEqual(CreateRequest.Value.MerchantSite.Details.AccountIBAN);
        };

        private It create_should_have_the_correct_account_swift = () => {
            CreateApiResult.Value.MerchantSite.Details.AccountSWIFT.ShouldEqual(CreateRequest.Value.MerchantSite.Details.AccountSWIFT);
        };

        private It create_should_have_the_correct_bank_swift = () => {
            CreateApiResult.Value.MerchantSite.Details.BankSWIFTID.ShouldEqual(CreateRequest.Value.MerchantSite.Details.BankSWIFTID);
        };

        private It create_should_have_the_correct_bank_code = () => {
            CreateApiResult.Value.MerchantSite.Details.BankCode.ShouldEqual(CreateRequest.Value.MerchantSite.Details.BankCode);
        };

        private It create_should_have_the_correct_vat_number = () => {
            CreateApiResult.Value.MerchantSite.Details.VATNumber.ShouldEqual(CreateRequest.Value.MerchantSite.Details.VATNumber);
        };

        private It create_should_have_the_correct_regsistration_number = () => {
            CreateApiResult.Value.MerchantSite.Details.RegistrationNumber.ShouldEqual(CreateRequest.Value.MerchantSite.Details.RegistrationNumber);
        };

        private It create_should_have_the_correct_account_mcc = () => {
            CreateApiResult.Value.MerchantSite.Details.MCC.ShouldEqual(CreateRequest.Value.MerchantSite.Details.MCC);
        };

        private It create_should_have_the_correct_main_business = () => {
            CreateApiResult.Value.MerchantSite.Details.MainBusiness.ShouldEqual(CreateRequest.Value.MerchantSite.Details.MainBusiness);
        };
    }
}
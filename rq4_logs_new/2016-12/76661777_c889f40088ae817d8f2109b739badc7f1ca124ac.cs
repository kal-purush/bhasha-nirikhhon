// <copyright file="CallbackData.cs">
//     Copyright (c) Andrey Igumnov. All rights reserved.
//     Licensed under the MIT License. See LICENSE file in the project root for full license information.
// </copyright>

namespace PayOnline.Form.SDK
{
    using System;
    using System.Collections.Specialized;
    using System.Globalization;
    using System.Net;
    using System.Security;

    /// <summary>
    /// Represents callback data
    /// </summary>
    public sealed class CallbackData
    {
        /// <summary>
        ///  Initializes a new instance of the <see cref="CallbackData"/> class
        /// </summary>
        /// <param name="callbackQuery">Callback query</param>
        /// <param name="merchantSettings">Merchant settings</param>
        public CallbackData(NameValueCollection callbackQuery, MerchantSettings merchantSettings)
        {
            if (callbackQuery == null)
            {
                throw new ArgumentNullException(nameof(callbackQuery));
            }

            this.TransactionId = ParseInt(callbackQuery, "TransactionId");
            this.TransactionDate = ParseDateTime(callbackQuery, "DateTime");
            this.OrderInfo = new OrderInfo(callbackQuery["OrderId"], ParseDecimal(callbackQuery, "Amount"), callbackQuery["Currency"]);
            this.PaymentAmount = ParseDecimal(callbackQuery, "PaymentAmount");
            this.PaymentCurrency = callbackQuery["PaymentCurrency"];
            this.MaskedCardNumber = callbackQuery["CardNumber"];
            this.CardholderName = callbackQuery["CardHolder"];
            this.Issuer = callbackQuery["BankName"];
            this.AuthorizationCode = callbackQuery["AuthCode"];
            this.BankTransactionId = callbackQuery["GatewayTransactionId"];
            this.EcommerceIndicator = GetEcommerceIndicator(callbackQuery["ECI"]);
            this.CardEnrollmentStatus = GetCardEnrollmentStatus(callbackQuery["ThreedsEnrollment"]);
            this.CountryCode = callbackQuery["Country"];
            this.City = callbackQuery["City"];
            this.StreetAddress = callbackQuery["Address"];
            this.ZipCode = callbackQuery["Zip"];
            this.Phone = callbackQuery["Phone"];
            this.Email = callbackQuery["Email"];
            IPAddress ip;
            this.IPAddress = IPAddress.TryParse(callbackQuery["IpAddress"], out ip) ? ip : null;
            this.IPCountryCode = callbackQuery["IpCountry"];
            this.BinCountryCode = callbackQuery["BinCountry"];
            this.SpecialConditions = callbackQuery["SpecialConditions"];
            this.RebillAnchor = callbackQuery["RebillAnchor"];
            this.DeclineCode = string.IsNullOrEmpty(callbackQuery["Code"]) ? (int?)null : ParseInt(callbackQuery, "Code");
            this.ErrorCode = string.IsNullOrEmpty(callbackQuery["ErrorCode"]) ? (int?)null : ParseInt(callbackQuery, "ErrorCode");

            var securityKey = new SecurityKey(merchantSettings, this.OrderInfo, this.TransactionId, this.TransactionDate).Value;
            if (callbackQuery["SecurityKey"] != securityKey)
            {
                throw new SecurityException(FormattableString.Invariant($"Security key mismatch. Expected: {securityKey}, but got: {callbackQuery["SecurityKey"]}"));
            }
        }

        /// <summary>
        /// Gets transaction ID
        /// </summary>
        public int TransactionId { get; }

        /// <summary>
        /// Gets transaction date
        /// </summary>
        public DateTime TransactionDate { get; }

        /// <summary>
        /// Gets order information
        /// </summary>
        public OrderInfo OrderInfo { get; }

        /// <summary>
        /// Gets payment amount
        /// </summary>
        public decimal PaymentAmount { get; }

        /// <summary>
        /// Gets payment currency in ISO 4217 format (e.g. USD, EUR)
        /// </summary>
        public string PaymentCurrency { get; }

        /// <summary>
        /// Gets masked card number
        /// </summary>
        public string MaskedCardNumber { get; }

        /// <summary>
        /// Gets cardholder name
        /// </summary>
        public string CardholderName { get; }

        /// <summary>
        /// Gets issuer bank name
        /// </summary>
        public string Issuer { get; }

        /// <summary>
        /// Gets bank authorization code
        /// </summary>
        public string AuthorizationCode { get; }

        /// <summary>
        /// Gets transaction ID in bank
        /// </summary>
        public string BankTransactionId { get; }

        /// <summary>
        /// Gets e-commerce indicator
        /// </summary>
        public EcommerceIndicator EcommerceIndicator { get; }

        /// <summary>
        /// Gets card 3D-S enrollment status
        /// </summary>
        public CardEnrollmentStatus CardEnrollmentStatus { get; }

        /// <summary>
        /// Gets billing country code according to ISO 3166-alpha2
        /// </summary>
        public string CountryCode { get; }

        /// <summary>
        ///  Gets city
        /// </summary>
        public string City { get; }

        /// <summary>
        /// Gets street address
        /// </summary>
        public string StreetAddress { get; }

        /// <summary>
        /// Gets zip code
        /// </summary>
        public string ZipCode { get; }

        /// <summary>
        /// Gets customer phone
        /// </summary>
        public string Phone { get; }

        /// <summary>
        /// Gets customer email
        /// </summary>
        public string Email { get; }

        /// <summary>
        /// Gets customer IP address
        /// </summary>
        public IPAddress IPAddress { get; }

        /// <summary>
        /// Gets country code, determined according to IP-address
        /// </summary>
        public string IPCountryCode { get; }

        /// <summary>
        /// Gets country code, determined according to bank bin
        /// </summary>
        public string BinCountryCode { get; }

        /// <summary>
        ///  Gets transaction special conditions
        /// </summary>
        public string SpecialConditions { get; }

        /// <summary>
        /// Gets link to repeated payments via this card
        /// </summary>
        public string RebillAnchor { get; }

        /// <summary>
        /// Gets transaction decline code
        /// </summary>
        public int? DeclineCode { get; }

        /// <summary>
        /// Gets error code
        /// </summary>
        public int? ErrorCode { get; }

        /// <summary>
        /// Parses integer value from query string
        /// </summary>
        /// <param name="query">Query string</param>
        /// <param name="parameterName">Query parameter name</param>
        /// <returns>Integer value</returns>
        private static int ParseInt(NameValueCollection query, string parameterName)
        {
            int value;
            if (!int.TryParse(query[parameterName], NumberStyles.None, CultureInfo.InvariantCulture, out value))
            {
                throw new ArgumentException(FormattableString.Invariant($"Callback query does not contain {parameterName} value"));
            }

            return value;
        }

        /// <summary>
        /// Parses date and time value from query string
        /// </summary>
        /// <param name="query">Query string</param>
        /// <param name="parameterName">Query parameter name</param>
        /// <returns>Date time value</returns>
        private static DateTime ParseDateTime(NameValueCollection query, string parameterName)
        {
            DateTime value;
            if (!DateTime.TryParseExact(query[parameterName], "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out value))
            {
                throw new ArgumentException(FormattableString.Invariant($"Callback query does not contain {parameterName} value"));
            }

            return value;
        }

        /// <summary>
        /// Parses decimal value from query string
        /// </summary>
        /// <param name="query">Query string</param>
        /// <param name="parameterName">Query parameter name</param>
        /// <returns>Decimal value</returns>
        private static decimal ParseDecimal(NameValueCollection query, string parameterName)
        {
            decimal value;
            if (!decimal.TryParse(query[parameterName], NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out value))
            {
                throw new ArgumentException(FormattableString.Invariant($"Callback query does not contain {parameterName} value"));
            }

            return value;
        }

        /// <summary>
        /// Gets e-commerce indicator
        /// </summary>
        /// <param name="ecommerceIndicatorValue">E-commerce indicator value</param>
        /// <returns>E-commerce indicator</returns>
        private static EcommerceIndicator GetEcommerceIndicator(string ecommerceIndicatorValue)
        {
            switch (ecommerceIndicatorValue)
            {
                case "5":
                    return EcommerceIndicator.FullThreeds;
                case "6":
                    return EcommerceIndicator.IssuerResponsibilityNonFullThreeds;
                case "7":
                    return EcommerceIndicator.MerchantResponsibility;
                default:
                    return EcommerceIndicator.None;
            }
        }

        /// <summary>
        /// Gets card enrollment status
        /// </summary>
        /// <param name="threedsEnrollment">Query parameter</param>
        /// <returns>Card enrollment status</returns>
        private static CardEnrollmentStatus GetCardEnrollmentStatus(string threedsEnrollment)
        {
            switch (threedsEnrollment)
            {
                case "1":
                    return CardEnrollmentStatus.Yes;
                case "0":
                    return CardEnrollmentStatus.No;
                case "2":
                    return CardEnrollmentStatus.Unavailable;
                default:
                    return CardEnrollmentStatus.Unknown;
            }
        }
    }
}
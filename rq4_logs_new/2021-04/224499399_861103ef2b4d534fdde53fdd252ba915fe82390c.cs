namespace TransactionProcessor.Common.Examples
{
    using System;

    /// <summary>
    /// 
    /// </summary>
    internal static class ExampleData
    {
        #region Fields

        /// <summary>
        /// The contract identifier
        /// </summary>
        internal static Guid ContractId = Guid.Parse("64CB4CFC-F7DF-411C-9FB3-A631478B503C");

        /// <summary>
        /// The customer email address
        /// </summary>
        internal static String CustomerEmailAddress = "exmaplecustomer@email.com";

        /// <summary>
        /// The device identifier
        /// </summary>
        internal static String DeviceIdentifier = "exampledeviceidentifier1";

        /// <summary>
        /// The estate identifier
        /// </summary>
        internal static Guid EstateId = Guid.Parse("9B3D0726-D1FC-45CE-BBB4-18F83EB93F07");

        /// <summary>
        /// The estate identifier metadata name
        /// </summary>
        internal static String EstateIdMetadataName = "EstateId";

        /// <summary>
        /// The logon response code
        /// </summary>
        internal static String LogonResponseCode = "0000";

        /// <summary>
        /// The logon response message
        /// </summary>
        internal static String LogonResponseMessage = "SUCCESS";

        /// <summary>
        /// The merchant identifier
        /// </summary>
        internal static Guid MerchantId = Guid.Parse("9B3D0726-D1FC-45CE-BBB4-18F83EB93F07");

        /// <summary>
        /// The merchant identifier metadata name
        /// </summary>
        internal static String MerchantIdMetadataName = "MerchantId";

        /// <summary>
        /// The operator identifier
        /// </summary>
        internal static String OperatorIdentifier = "Safaricom";

        /// <summary>
        /// The product identifier
        /// </summary>
        internal static Guid ProductId = Guid.Parse("C0AEC683-587E-4F6A-BB20-B36312D9F712");

        /// <summary>
        /// The reconciliation response code
        /// </summary>
        internal static String ReconciliationResponseCode = "0000";

        /// <summary>
        /// The reconciliation response message
        /// </summary>
        internal static String ReconciliationResponseMessage = "SUCCESS";

        /// <summary>
        /// The sale response code
        /// </summary>
        internal static String SaleResponseCode = "0000";

        /// <summary>
        /// The sale response message
        /// </summary>
        internal static String SaleResponseMessage = "SUCCESS";

        /// <summary>
        /// The transaction count
        /// </summary>
        internal static Int32 TransactionCount = 1;

        /// <summary>
        /// The transaction date time
        /// </summary>
        internal static DateTime TransactionDateTime = DateTime.Now;

        /// <summary>
        /// The transaction number
        /// </summary>
        internal static String TransactionNumber = "1";

        /// <summary>
        /// The transaction type logon
        /// </summary>
        internal static String TransactionTypeLogon = "Logon";

        /// <summary>
        /// The transaction type sale
        /// </summary>
        internal static String TransactionTypeSale = "Sale";

        /// <summary>
        /// The transaction value
        /// </summary>
        internal static Decimal TransactionValue = 10.00m;

        #endregion
    }
}
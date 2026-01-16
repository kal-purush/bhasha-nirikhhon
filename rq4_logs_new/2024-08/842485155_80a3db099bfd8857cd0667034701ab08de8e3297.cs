using CurrencyCloud.Entity;
using CurrencyCloud.Entity.List;
using CurrencyCloud.Entity.Pagination;
using CurrencyCloud.Environment;

namespace CurrencyCloud;

public interface ICurrencyCloudClient
{
    /// <summary>
    /// Executes operations on behalf of another contact.
    /// </summary>
    /// <param name="id">Id of the contact.</param>
    /// <param name="function">Asynchronous function, which is executed on behalf of the given contact.</param>
    /// <returns>Asynchronous task.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the previous call of the method has not yet completed.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="id"/> is not a valid UUID.</exception>
    Task OnBehalfOf(string id, Func<Task> function);

    /// <summary>
    /// Gets a value that indicates whether the client is initialized.
    /// </summary>
    bool IsInitialized { get; }

    /// <summary>
    /// Initializes the client and generates authentication token for the API user.
    /// </summary>
    /// <param name="apiServer">API server to make requests against.</param>
    /// <param name="loginId">Login id of the API user.</param>
    /// <param name="apiKey">API key of the API user.</param>
    /// <returns>Asynchronous task, which returns the authentication token.</returns>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<string> InitializeAsync(ApiServer apiServer, string loginId, string apiKey);

    /// <summary>
    /// Closes current session and resets the client.
    /// </summary>
    /// <returns>Asynchronous task.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task CloseAsync();

    /// <summary>
    /// Creates a new account.
    /// </summary>
    /// <param name="account">Account object</param>
    /// <returns>Asynchronous task, which returns newly created account.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<Account> CreateAccountAsync(Account account);

    /// <summary>
    /// Gets details of an account.
    /// </summary>
    /// <param name="id">Id of the requested account.</param>
    /// <returns>Asynchronous task, which returns the requested account.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<Account> GetAccountAsync(string id);

    /// <summary>
    /// Updates an existing account.
    /// </summary>
    /// <param name="account">Account object to be updated</param>
    /// <returns>Asynchronous task, which returns the updated account.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<Account> UpdateAccountAsync(Account account);

    /// <summary>
    /// Finds accounts matching the search criteria for the active user.
    /// </summary>
    /// <param name="parameters">Find parameters</param>
    /// <returns>Asynchronous task, which returns the list of the found accounts, as well as pagination information.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaginatedAccounts> FindAccountsAsync(AccountFindParameters parameters = null);

    /// <summary>
    /// Gets details of the active account.
    /// </summary>
    /// <returns>Asynchronous task, which returns the active account.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<Account> GetCurrentAccountAsync();

    /// <summary>
    /// Gets payment charges settings for given account.
    /// </summary>
    /// <returns>Asynchronous task, which returns the payment charges settings.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaymentChargesSettingsList> GetPaymentChargesSettingsAsync(string id);

    /// <summary>
    /// Manage given Account's Payment Charge Settings (enable, disable, set default).
    /// </summary>
    /// <param name="paymentChargesSettings">Account's Payment Charge Settings object to be updated</param>
    /// <returns>Asynchronous task, which returns the updated account.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaymentChargesSettings> ManageAccountPaymentChargesSettingsAsync(PaymentChargesSettings paymentChargesSettings);

    /// <summary>
    /// Gets the balance for a currency.
    /// </summary>
    /// <param name="currency">Currency to get the balance for.</param>
    /// <returns>Asynchronous task, which returns the requested balance.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<Balance> GetBalanceAsync(string currency);

    /// <summary>
    /// Finds balances matching the search criteria.
    /// </summary>
    /// <param name="parameters">Find parameters</param>
    /// <returns>Asynchronous task, which returns the list of the found balances, as well as pagination information.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaginatedBalances> FindBalancesAsync(BalanceFindParameters parameters = null);

    /// <summary>
    /// Top up the margin balance for a currency.
    /// </summary>
    /// <param name="currency">Currency to top up the balance with.</param>
    /// <param name="amount">Amount to top up the balance with.</param>
    /// <returns>Asynchronous task, which tops up the given margin balance.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<MarginBalanceTopUp> TopUpMarginBalanceAsync(string currency, decimal amount);

    /// <summary>
    /// Validates beneficiary details without creating one.
    /// </summary>
    /// <param name="beneficiary">Beneficiary data to be validated</param>
    /// <returns>Asynchronous task, which returns the validated beneficiary.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<Beneficiary> ValidateBeneficiaryAsync(Beneficiary beneficiary);

    /// <summary>
    /// Creates a new beneficiary.
    /// </summary>
    /// <param name="beneficiary">Beneficiary object to be created</param>
    /// <returns>Asynchronous task, which returns newly created beneficiary.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<Beneficiary> CreateBeneficiaryAsync(Beneficiary beneficiary);

    /// <summary>
    /// Gets details of a beneficiary.
    /// </summary>
    /// <param name="id">Id of the requested beneficiary.</param>
    /// <returns>Asynchronous task, which returns the requested beneficiary.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<Beneficiary> GetBeneficiaryAsync(string id);

    /// <summary>
    /// Updates an existing beneficiary.
    /// </summary>
    /// <param name="beneficiary">Beneficiary object to be updated</param>
    /// <returns>Asynchronous task, which returns the updated beneficiary.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    /// <exception cref="ArgumentException">Thrown if Beneficiary.Id is NULL</exception>
    Task<Beneficiary> UpdateBeneficiaryAsync(Beneficiary beneficiary);

    /// <summary>
    /// Finds beneficiaries matching the search criteria for the active user.
    /// </summary>
    /// <param name="parameters">Find parameters</param>
    /// <returns>Asynchronous task, which returns the list of the found beneficiaries, as well as pagination information.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaginatedBeneficiaries> FindBeneficiariesAsync(BeneficiaryFindParameters parameters = null);

    /// <summary>
    /// Deletes an existing beneficiary.
    /// </summary>
    /// <param name="id">Id of the deleted beneficiary.</param>
    /// <returns>Asynchronous task, which returns the deleted beneficiary.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<Beneficiary> DeleteBeneficiaryAsync(string id);

    /// <summary>
    /// Creates a new contact.
    /// </summary>
    /// <param name="contact">Contact object to be created</param>
    /// <returns>Asynchronous task, which returns newly created contact.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<Contact> CreateContactAsync(Contact contact);

    /// <summary>
    /// Gets details of a contact.
    /// </summary>
    /// <param name="id">Id of the requested contact.</param>
    /// <returns>Asynchronous task, which returns the requested contact.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<Contact> GetContactAsync(string id);

    /// <summary>
    /// Updates an existing contact.
    /// </summary>
    /// <param name="contact">Contact object to be updated</param>
    /// <returns>Asynchronous task, which returns the updated contact.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    /// <exception cref="ArgumentException">Thrown if Contact.Id is NULL</exception>
    Task<Contact> UpdateContactAsync(Contact contact);

    /// <summary>
    /// Finds contacts matching the search criteria for the active user.
    /// </summary>
    /// <param name="parameters">Find parameters</param>
    /// <returns>Asynchronous task, which returns the list of the found contacts, as well as pagination information.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaginatedContacts> FindContactsAsync(ContactFindParameters parameters = null);

    /// <summary>
    /// Gets details of the active contact.
    /// </summary>
    /// <returns>Asynchronous task, which returns the active contact.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<Contact> GetCurrentContactAsync();

    /// <summary>
    /// Creates a new conversion.
    /// </summary>
    /// <param name="conversion">Data object for new conversion</param>
    /// <returns>Asynchronous task, which returns newly created conversion.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<Conversion> CreateConversionAsync(Conversion conversion);

    /// <summary>
    /// Gets details of a conversion.
    /// </summary>
    /// <param name="id">Id of the requested conversion.</param>
    /// <returns>Asynchronous task, which returns the requested conversion.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<Conversion> GetConversionAsync(string id);

    /// <summary>
    /// Finds conversions matching the search criteria for the active user.
    /// </summary>
    /// <param name="parameters">Find parameters</param>
    /// <returns>Asynchronous task, which returns the list of the found conversions, as well as pagination information.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaginatedConversions> FindConversionsAsync(ConversionFindParameters parameters = null);

    /// <summary>
    /// Quotes cost of cancelling conversion identified by the provided unique id.
    /// </summary>
    /// <param name="conversionCancellationQuote">Object holding the Id of the conversion that is being quoted</param>
    /// <returns>Asynchronous task, which returns the details of the cancelled conversion</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<ConversionCancellation> QuoteCancelConversionAsync(ConversionCancellation conversionCancellationQuote);

    /// <summary>
    /// Cancels the conversion identified by the provided unique id.
    /// </summary>
    /// <param name="conversionCancellation">Object holding the Id and Notes (optional) of the conversion that is being cancelled</param>
    /// <returns>Asynchronous task, which returns the details of the cancelled conversion</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<ConversionCancellation> CancelConversionsAsync(ConversionCancellation conversionCancellation);

    /// <summary>
    /// Returns an object containing the quote for changing the date of the specified conversion.
    /// </summary>
    /// <param name="conversionDateChange">Object holding the Id and New Settlement Date of the conversion that is being changed</param>
    /// <returns>Asynchronous task, which returns the details of the conversion date change</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<ConversionDateChange> QuoteDateChangeConversionAsync(ConversionDateChange conversionDateChange);

    /// <summary>
    /// Changes the date ofthe conversion identified by the provided unique id.
    /// </summary>
    /// <param name="conversionDateChange">Object holding the Id and New Settlement Date of the conversion that is being changed</param>
    /// <returns>Asynchronous task, which returns the details of the conversion date change</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<ConversionDateChange> DateChangeConversionAsync(ConversionDateChange conversionDateChange);

    /// <summary>
    /// Previews a conversion split.
    /// </summary>
    /// <param name="conversionSplit">Object holding the Id and Amount of the conversion to preview a split</param>
    /// <returns>Asynchronous task, which returns the details of the split conversion</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<ConversionSplit> PreviewSplitConversionAsync(Conversion conversionSplit);

    /// <summary>
    /// Splits a conversion.
    /// </summary>
    /// <param name="conversionSplit">Object holding the Id and Amount of the conversion to split</param>
    /// <returns>Asynchronous task, which returns the details of the split conversion</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<ConversionSplit> SplitConversionAsync(Conversion conversionSplit);

    /// <summary>
    /// Conversion split history.
    /// </summary>
    /// <param name="conversionSplit">Object holding the Id of the conversion to query</param>
    /// <returns>Asynchronous task, which returns the split history for a given conversion</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<ConversionSplitHistory> SplitHistoryConversionAsync(Conversion conversionSplit);

    /// <summary>
    /// Returns an object that contains information related to actions on conversions that have generated profit or loss
    /// </summary>
    /// <param name="parameters">Find parameters</param>
    /// <returns>Asynchronous task, which returns the list of the found conversions, as well as pagination information.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaginatedConversionProfitAndLosses> FindConversionProfitAndLossesAsync(ConversionProfitAndLossFindParameters parameters = null);

    /// <summary>
    /// Returns an object that contains information related to Funding Accounts
    /// </summary>
    /// <param name="parameters">Find parameters</param>
    /// <returns>Asynchronous task, which returns the list of the funding accounts, as well as pagination information.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaginatedFundingAccounts> FindFundingAccountsAsync(FundingAccountFindParameters parameters = null);

    /// <summary>
    /// Find IBANs assigned to the logged in account.
    /// </summary>
    /// <param name="parameters">Find parameters</param>
    /// <returns>Asynchronous task, which returns structure containing the details of the IBAN assigned to the logged in account.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaginatedIbans> FindIbansAsync(IbanFindParameters parameters = null);

    /// <summary>
    /// Gets details of a payer.
    /// </summary>
    /// <param name="id">Id of the requested payer.</param>
    /// <returns>Asynchronous task, which returns the requested payer.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<Payer> GetPayerAsync(string id);

    /// <summary>
    /// Creates a new payment.
    /// </summary>
    /// <param name="payment">Payment object to be created</param>
    /// <param name="payer">Optional payer info</param>
    /// <returns>Asynchronous task, which returns newly created payment.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    /// <remarks>Payment.PayerDetailsSource not passed to server while creation</remarks>
    Task<Payment> CreatePaymentAsync(Payment payment, Payer payer = null);

    /// <summary>
    /// Gets details of a payment.
    /// </summary>
    /// <param name="id">Id of the requested payment.</param>
    /// <returns>Asynchronous task, which returns the requested payment.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<Payment> GetPaymentAsync(string id);

    /// <summary>
    /// Updates an existing payment.
    /// </summary>
    /// <param name="payment">Payment object to be updated</param>
    /// <param name="payer">Optional payer data to be updated for payment</param>
    /// <returns>Asynchronous task, which returns the updated payment.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    /// <exception cref="ArgumentException">Thrown when Payment.Id is NULL</exception>
    Task<Payment> UpdatePaymentAsync(Payment payment, Payer payer = null);

    /// <summary>
    /// Finds payments matching the search criteria for the active user.
    /// </summary>
    /// <param name="parameters">Find parameters</param>
    /// <returns>Asynchronous task, which returns  the list of the found payments, as well as pagination information.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaginatedPayments> FindPaymentsAsync(FindParameters parameters = null);

    /// <summary>
    /// Deletes an existing payment.
    /// </summary>
    /// <param name="id">Id of the deleted payment.</param>
    /// <returns>Asynchronous task, which returns the deleted payment.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<Payment> DeletePaymentAsync(string id);

    /// <summary>
    /// Returns a hash containing the details of MT103 information for a SWIFT payments.
    /// </summary>
    /// <param name="id">Id payment.</param>
    /// <returns>Asynchronous task, which returns the MT103 information for a SWIFT payment.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaymentSubmission> GetPaymentSubmissionAsync(string id);

    /// <summary>
    /// Returns an array of PaymentAuthorisation Objects
    /// </summary>
    /// <param name="paymentIds">Array of Payment Ids to authorise</param>
    /// <returns>Asynchronous task, which returns an array of PaymentAuthorisation Objects</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaymentAuthorisationsList> PaymentAuthorisationAsync(string[] paymentIds);

    /// <summary>
    /// Returns an object containing the confirmation details of a payment.
    /// </summary>
    /// <param name="id">Id of payment.</param>
    /// <returns>Asynchronous task, which returns the confirmation details of a payment.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaymentConfirmation> GetPaymentConfirmationAsync(string id);

    /// <summary>
    /// Returns an object containing the expected payment delivery date.
    /// </summary>
    /// <param name="paymentDeliveryDate">paymentDeliveryDate to query.</param>
    /// <returns>Asynchronous task, which returns the confirmation details of a payment.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaymentDeliveryDates> GetPaymentDeliveryDatesAsync(PaymentDeliveryDates paymentDeliveryDates);

    /// <summary>
    /// Gets the calculated quote for the fee that will be applied against a payment
    /// </summary>
    /// <param name="quotePaymentFee">Quote Payment Fee Details</param>
    /// <returns>Asynchronous task, which returns the Quote Payment Fee.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<QuotePaymentFee> GetQuotePaymentFee(QuotePaymentFee quotePaymentFee);

    /// <summary>
    /// Returns an object containing the tracking information of a payment.
    /// </summary>
    /// <param name="id">Id of payment.</param>
    /// <returns>Asynchronous task, which returns the tracking info of a payment.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaymentTrackingInfo> GetPaymentTrackingInfoAsync(string id);

    /// <summary>
    /// Gets a full quote for the requested currency based on the spread table of the active contact.
    /// </summary>
    /// <param name="detailedRates">Rate parameters object</param>
    /// <returns>Asynchronous task, which returns the requested rate.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<Rate> GetRateAsync(DetailedRates detailedRates);

    /// <summary>
    /// Gets core rate information for multiple currency pairs.
    /// </summary>
    /// <param name="currencyPair">Currency pair</param>
    /// <param name="ignoreInvalidPairs">Optional: Ignore invalid pairs</param>
    /// <returns>Asynchronous task, which returns the list of the found rates.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<RatesList> FindRatesAsync(string currencyPair, bool? ignoreInvalidPairs = null);

    /// <summary>
    /// Gets required beneficiary details and their basic validation formats.
    /// </summary>
    /// <param name="currency">Currency</param>
    /// <param name="bankAccountCountry">Optional: Bank account country</param>
    /// <param name="beneficiaryCountry">Optional: Beneficiary country</param>
    /// <returns>Asynchronous task, which returns the list of the required beneficiary details.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<BeneficiaryDetailsList> GetBeneficiaryRequiredDetailsAsync(string currency = null, string bankAccountCountry = null, string beneficiaryCountry = null);

    /// <summary>
    /// Gets dates for which the given currency pair can not be traded.
    /// </summary>
    /// <param name="conversionPair">Currency conversion pair.</param>
    /// <param name="startDate">Optional: start date</param>
    /// <returns>Asynchronous task, which returns the list of the conversion dates.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<ConversionDatesList> GetConversionDatesAsync(string conversionPair, DateTime? startDate = null);

    /// <summary>
    /// Gets a list of all the currencies that are tradeable.
    /// </summary>
    /// <returns>Asynchronous task, which returns the list of the currencies.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<CurrenciesList> GetAvailableCurrenciesAsync();

    /// <summary>
    /// Gets a list of purpose codes for a given currency.
    /// </summary>
    /// <param name="currency">Currency to get the purpose codes for</param>
    /// <param name="entityType">Optional: entity (individual or company)</param>
    /// <param name="bankAccountCountry">Optional: bank account country</param>
    /// <returns>Asynchronous task, which returns the list purpose codes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaymentPurposeCodeList> GetPaymentPurposeCodes(string currency, string bankAccountCountry, string entityType = null);

    /// <summary>
    /// Gets dates for which the given currency can not be paid.
    /// </summary>
    /// <param name="currency">Currency name.</param>
    /// <param name="startDate">Start date</param>
    /// <returns>Asynchronous task, which returns the list of the payment dates.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaymentDatesList> GetPaymentDatesAsync(string currency, DateTime? startDate = null);

    /// <summary>
    /// Gets settlement account information, detailing where funds need to be sent to.
    /// </summary>
    /// <param name="currency">Currency</param>
    /// <returns>Asynchronous task, which returns the list of the found rates.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<SettlementAccountsList> GetSettlementAccountsAsync(string currency = null);

    /// <summary>
    /// Gets required payer details and their basic validation formats.
    /// </summary>
    /// <param name="payerCountry">ISO 3166-1 country code</param>
    /// <param name="payerEntityType">Optional: Payer Entity Type (could be company or individual)</param>
    /// <param name="paymentType">Optional: Payment Type (could be priority or regular)</param>
    /// <returns>Asynchronous task, which returns required payer details and their basic validation formats.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PayerDetailsList> GetPayerRequiredDetailsAsync(string payerCountry, string payerEntityType = null, string paymentType = null);

    /// <summary>
    /// Gets Details of the bank associated to specified account.
    /// </summary>
    /// <param name="identifierType">IdentifierType</param>
    /// <param name="identifierValue">IdentifierValue</param>
    /// <returns>Asynchronous task, which returns the Bank Details.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<BankDetails> GetBankDetailsAsync(string identifierType, string identifierValue);

    /// <summary>
    /// Gets Payment Fee Rules.
    /// </summary>
    /// <param name="accountId">AccountId</param>
    /// <param name="paymentType">PaymentType</param>
    /// <param name="chargeType">ChargeType</param>
    /// <returns>Asynchronous task, which returns the Bank Details.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaymentFeeRulesList> GetPaymentFeeRulesAsync(string accountId=null, string paymentType=null, string chargeType=null);

    /// <summary>
    /// Finds report requests matching the given search criteria.
    /// </summary>
    /// <param name="parameters">Find parameters</param>
    /// <returns>Asynchronous task, which returns  the list of the report requests, as well as pagination information.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaginatedReportRequests> FindReportRequestsAsync(ReportRequestFindParameters parameters = null);

    /// <summary>
    /// Gets details of the specified report request.
    /// </summary>
    /// <param name="id">Id of the requested report.</param>
    /// <returns>Asynchronous task, which returns the requested transaction.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<ReportRequest> GetReportRequestAsync(string id);

    /// <summary>
    /// Creates a new Conversion Report.
    /// </summary>
    /// <param name="parameters">Parameters for new Report</param>
    /// <returns>Asynchronous task, which returns newly created conversion.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<ReportRequest> CreateConversionReportAsync(ReportParameters parameters = null);

    /// <summary>
    /// Creates a new Payment Report.
    /// </summary>
    /// <param name="parameters">Parameters for new Report</param>
    /// <returns>Asynchronous task, which returns newly created conversion.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<ReportRequest> CreatePaymentReportAsync(ReportParameters parameters = null);

    /// <summary>
    /// Gets details of a transaction.
    /// </summary>
    /// <param name="id">Id of the requested transaction.</param>
    /// <returns>Asynchronous task, which returns the requested transaction.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<Transaction> GetTransactionAsync(string id);

    /// <summary>
    /// Finds transactions matching the search criteria for the active user.
    /// </summary>
    /// <param name="parameters">Find parameters</param>
    /// <returns>Asynchronous task, which returns the list of the found transactions, as well as pagination information.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaginatedTransactions> FindTransactionsAsync(TransactionFindParameters parameters = null);

    /// <summary>
    /// Get Sender Details.
    /// </summary>
    /// <param name="id">Id of the requested transaction.</param>
    /// <returns>Asynchronous task, which returns details of the sender of funds.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<SenderDetails> GetSenderDetailsAsync(string id);

    /// <summary>
    /// Creates a new Transfer.
    /// </summary>
    /// <param name="transfer">Data object for new Transfer</param>
    /// <returns>Asynchronous task, which returns newly created conversion.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<Transfer> CreateTransferAsync(Transfer transfer);

    /// <summary>
    /// Gets details of a transfer.
    /// </summary>
    /// <param name="id">Id of the requested conversion.</param>
    /// <returns>Asynchronous task, which returns the requested conversion.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<Transfer> GetTransferAsync(string id);

    /// <summary>
    /// Find all Transfers matching the given search criteria
    /// </summary>
    /// <param name="parameters">Find parameters</param>
    /// <returns>Asynchronous task, which returns structure containing the details of the IBAN assigned to the logged in account.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaginatedTransfers> FindTransfersAsync(TransferFindParameters parameters = null);

    /// <summary>
    /// Cancels a transfer.
    /// </summary>
    /// <param name="id">Id of the transfer to be cancelled.</param>
    /// <returns>Asynchronous task, which returns the cancelled conversion.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<Transfer> CancelTransferAsync(string id);

    /// <summary>
    /// Find Virtual Accounts assigned to the logged in account.
    /// </summary>
    /// <param name="parameters">Find parameters</param>
    /// <returns>Asynchronous task, which returns the details of the Virtual Accounts assigned to the logged in account.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaginatedVirtualAccounts> FindVirtualAccountsAsync(VirtualAccountFindParameters parameters = null);

    /// <summary>
    /// Finds Withdrawal Accounts matching the accountId. If the account Id is omitted the withdrawal accounts
    /// for the house account and all sub-accounts are returned
    /// </summary>
    /// <param name="accountId">AccountId</param>
    /// <returns>Asynchronous task, which returns the list of the found Withdrawal Accounts, as well as pagination information.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<PaginatedWithdrawalAccounts> FindWithdrawalAccountsAsync(String accountId = null);

    /// <summary>
    /// Pull funds from a withdrawal account
    /// </summary>
    /// <param name="withdrawalAccountId">Id of withdrawal account to pull funds from.</param>
    /// <param name="amount">The amount of funds to pull</param>
    /// <param name="reference">The reference seen on the statement for pulled funds</param>
    /// <returns>Asynchronous task, which pulls funds from a withdrawal account.</returns>
    /// <exception cref="InvalidOperationException">Thrown when client is not initialized.</exception>
    /// <exception cref="ApiException">Thrown when API call fails.</exception>
    Task<WithdrawalAccountFunds> WithdrawalAccountsPullFundsAsync(string withdrawalAccountId, decimal amount,
        string reference);
}
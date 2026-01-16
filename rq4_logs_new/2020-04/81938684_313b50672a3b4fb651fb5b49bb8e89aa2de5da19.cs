using AssureCare.MC.ENF.Provider.Model.Interface;
using AssureCare.MC.Provider.ConsumerService.Application;
using AssureCare.MC.Provider.ConsumerService.BusinessLayer.Interfaces;
using AssureCare.MC.Provider.ConsumerService.Configuration;
using AssureCare.MC.Provider.ConsumerService.DataAccess.Enums;
using AssureCare.MC.Provider.ConsumerService.DataAccess.Interfaces;
using AssureCare.MC.Provider.ConsumerService.Helpers;
using AssureCare.MC.Provider.ConsumerService.Helpers.Interfaces;
using AssureCare.MC.Provider.ConsumerService.Models.Constants;
using AssureCare.MC.Provider.ConsumerService.Models.ErrorsPayload;
using AssureCare.MC.Provider.ConsumerService.Models.Interfaces;
using AssureCare.MC.Provider.ConsumerService.Models.Services;
using AssureCare.MC.Provider.ConsumerService.Services.Ria;
using AssureCare.MC.Provider.ConsumerService.Validators;
using Mc.Ria.Services.Soap.SoapService;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Ria = Mc.Ria.Services.Soap.SoapService;
using Assure = AssureCare.MC.Provider.ConsumerService.Models.Services;
using System.Net;
using System.Text;

namespace AssureCare.MC.Provider.ConsumerService.BusinessLayer
{
    public class ProviderDataBulkLoad : IProviderDataBulkLoad
    {
        private readonly IConfigurations _iconfiguration;
        private readonly MemberProviderValidator _pmemberProviderValidator;
        private readonly ProviderValidator _providerValidator;
        private readonly ILogRepository _logRepository;
        private readonly ProviderContractValidator _pContractValidator;
        private readonly ProviderEmailAddressValidator _providerEmailAddressValidator;
        private readonly MessageHeaderValidator _messageHeaderValidator;
        private readonly ProviderPhoneNumberValidator _providerPhoneNumberValidator;
        private readonly ProviderPostalAddressValidator _providerPostalAddressValidator;
        private readonly ProviderIdentifierValidator _providerIdentifierValidator;
        private readonly ProviderNetworkValidator _providerNetworkValidator;
        private readonly string _currentThreadId;
        private readonly string _titleForLog;
        private string _processId;
        private readonly IServiceProvider _riaServiceProvider;
        private readonly MCServices _mCServices;
        private readonly IGetTableInfo _getTableInfo;
        private readonly ProviderRiaModel _providerRiaModel;
        private readonly string _ParentProvider;
        private readonly string _ParentMember;
        private List<ErrorsModal> errorsModal = new List<ErrorsModal>();
        private readonly IResultData _resultData;
        private readonly ObjectsError _objectsError;
        private readonly ObjectsSuccess _objectsSuccess;
        private readonly string _CcxProviderId;
        private readonly string _eventTypeIdValueError;
        private readonly string _eventTypeIdValueAdd;
        private readonly string _eventTypeIdValueNotify;
        private readonly string _jsonInvalid;
        private string _providerId = string.Empty;
      

        #region Constractor
        public ProviderDataBulkLoad(
                        MessageHeaderValidator messageHeaderValidator,
                        MemberProviderValidator memberProviderValidator,
                        ProviderValidator pValidator,
                        ProviderContractValidator providerContractValidator,
                        ProviderEmailAddressValidator providerEmailAddressValidator,
                        ProviderPhoneNumberValidator providerPhoneNumberValidator,
                        ProviderPostalAddressValidator providerPostalAddressValidator,
                        ProviderIdentifierValidator providerIdentifierValidator,
                        ProviderNetworkValidator providerNetworkValidator,
                        ILogRepository logRepository,
                        IGetTableInfo getTableInfo,
                        ProviderRiaModel providerRiaModel,
                        IResultData resultData, ObjectsError objectsError, ObjectsSuccess objectsSuccess
                                   )
        {

            _pmemberProviderValidator = memberProviderValidator;
            _providerValidator = pValidator;
            _pContractValidator = providerContractValidator;
            _providerEmailAddressValidator = providerEmailAddressValidator;
            _providerPhoneNumberValidator = providerPhoneNumberValidator;
            _providerPostalAddressValidator = providerPostalAddressValidator;
            _providerIdentifierValidator = providerIdentifierValidator;
            _messageHeaderValidator = messageHeaderValidator;
            _providerNetworkValidator = providerNetworkValidator;
            _logRepository = logRepository;
            _iconfiguration = ConfigurationDetails.GetConfigDetails();
            _titleForLog = _iconfiguration.LogSettings.TitleForLog;
            _riaServiceProvider = DependencyBinder.RiaServiceProvider;
            _ParentProvider = _iconfiguration.PayLoadErrorCodes.ProviderCanNotFound;
            _ParentMember = _iconfiguration.PayLoadErrorCodes.MemberCanNotFound;
            _CcxProviderId = _iconfiguration.PayLoadErrorCodes.SourceProviderIdInvalidErrorCode;
            _eventTypeIdValueError = _iconfiguration.PayLoadErrorCodes.EventTypeIdInvalidErrorCode;
            _jsonInvalid = _iconfiguration.PayLoadErrorCodes.JsonInvalidErrorCode;
            _eventTypeIdValueAdd = _iconfiguration.BusSettings.Queueconfig.Add;
            _eventTypeIdValueNotify = _iconfiguration.BusSettings.Queueconfig.Notify;
            _mCServices = _riaServiceProvider.GetRequiredService<MCServices>();
            _getTableInfo = getTableInfo;
            _currentThreadId = Thread.CurrentThread.ManagedThreadId.ToString();
            _providerRiaModel = providerRiaModel;
            _resultData = resultData;
            _objectsError = objectsError;
            _objectsSuccess = objectsSuccess;
           
        }
        #endregion

        #region GetProviderQuery
        public async Task<bool> GetProviderQuery(IRBMQMessage rbmqMessages)
        {
            List<string> providerValidationResult = new List<string>();
            bool validationResult = false;
            errorsModal = new List<ErrorsModal>();
            _processId = rbmqMessages.EventTypeIdValue;
            try
            {
                _logRepository.AddLogDetails(NLog.LogLevel.Info, SeverityLevels.Information.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"DateTimeSent = {rbmqMessages.DateTimeSent}, " +
                                                    $"EventTypeIdValue = {rbmqMessages.EventTypeIdValue}, " +
                                                    $"EventIdSource = {rbmqMessages.EventIdSource}," +
                                                    $"EventIdValue = {rbmqMessages.EventIdValue}, " +
                                                    $"InsertedOn = {rbmqMessages.InsertedOn}");

                _resultData.Reqtranid = Guid.NewGuid().ToString();
                _resultData.Originalreqtranid = rbmqMessages.EventIdValue;
                _resultData.Reqtransentdatetime = rbmqMessages.DateTimeSent;
                _resultData.Originalsourceid = rbmqMessages.EventIdSource;
                _resultData.Datetimeprocessed = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fff");
                _resultData.Originalrequesttype = rbmqMessages.EventTypeIdValue;

                var headerValidation = _messageHeaderValidator.Validate(rbmqMessages);
                if (headerValidation.IsValid)
                {
                    var providerdata = JsonConvert.DeserializeObject<ProviderDetails>(rbmqMessages.Payload);
                    if (IsValidateFullorDiscreteJsonPayload(providerdata))
                    {
                        if (IsValidateProviderSourceId(providerdata))
                        {

                            if (providerdata.Provider != null)
                            {
                                validationResult = await ValidateAndSaveProvider(providerdata);
                                validationResult = (providerdata.Provider.ProviderPostalAddress != null && providerdata.Provider.ProviderPostalAddress.Count > 0) ? await ValidateAndSaveProviderPostalAddress(providerdata.Provider.ProviderPostalAddress) : false;
                                validationResult = (providerdata.Provider.MemberProvider != null && providerdata.Provider.MemberProvider.Count > 0) ? await ValidateAndSaveMemberProvider(providerdata.Provider.MemberProvider) : false;
                                validationResult = (providerdata.Provider.ProviderContract != null && providerdata.Provider.ProviderContract.Count > 0) ? await ValidateAndSaveProviderContract(providerdata.Provider.ProviderContract) : false;
                                validationResult = (providerdata.Provider.ProviderEmailAddress != null && providerdata.Provider.ProviderEmailAddress.Count > 0) ? await ValidateAndSaveProviderEmailAddress(providerdata.Provider.ProviderEmailAddress) : false;
                                validationResult = (providerdata.Provider.ProviderPhoneNumber != null && providerdata.Provider.ProviderPhoneNumber.Count > 0) ? await ValidateAndSaveProviderPhoneNumber(providerdata.Provider.ProviderPhoneNumber) : false;
                                validationResult = (providerdata.Provider.ProviderIdentifier != null && providerdata.Provider.ProviderIdentifier.Count > 0) ? await ValidateAndSaveProviderIdentifier(providerdata.Provider.ProviderIdentifier) : false;
                                validationResult = (providerdata.Provider.ProviderNetwork != null && providerdata.Provider.ProviderNetwork.Count > 0) ? await ValidateAndSaveProviderNetwork(providerdata.Provider.ProviderNetwork) : false;
                            }
                            else
                            {
                                validationResult = (providerdata.ProviderPostalAddress != null && providerdata.ProviderPostalAddress.Count > 0) ? await ValidateAndSaveProviderPostalAddress(providerdata.ProviderPostalAddress) : false;
                                validationResult = (providerdata.MemberProvider != null && providerdata.MemberProvider.Count > 0) ? await ValidateAndSaveMemberProvider(providerdata.MemberProvider) : false;
                                validationResult = (providerdata.ProviderContract != null && providerdata.ProviderContract.Count > 0) ? await ValidateAndSaveProviderContract(providerdata.ProviderContract) : false;
                                validationResult = (providerdata.ProviderEmailAddress != null && providerdata.ProviderEmailAddress.Count > 0) ? await ValidateAndSaveProviderEmailAddress(providerdata.ProviderEmailAddress) : false;
                                validationResult = (providerdata.ProviderPhoneNumber != null && providerdata.ProviderPhoneNumber.Count > 0) ? await ValidateAndSaveProviderPhoneNumber(providerdata.ProviderPhoneNumber) : false;
                                validationResult = (providerdata.ProviderIdentifier != null && providerdata.ProviderIdentifier.Count > 0) ? await ValidateAndSaveProviderIdentifier(providerdata.ProviderIdentifier) : false;
                                validationResult = (providerdata.ProviderNetwork != null && providerdata.ProviderNetwork.Count > 0) ? await ValidateAndSaveProviderNetwork(providerdata.ProviderNetwork) : false;
                            }

                        }
                        else
                        {
                            errorsModal.Add(new ErrorsModal()
                            {
                                Error_code = _CcxProviderId,
                                Error_desc = "SourceProviderId mismatch - Transaction Invalid"
                            });
                            _objectsError.providerError = new ProviderError()
                            {
                                Id = providerdata.Provider.SourceProviderId,
                                Errors = errorsModal
                            };
                            _logRepository.AddLogDetails(LogLevel.Error, SeverityLevels.FullCritical.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"{_CcxProviderId} - SourceProviderId mismatch - Transaction Invalid");
                        }
                    }
                    else
                    {
                        errorsModal.Add(new ErrorsModal()
                        {
                            Error_code = _jsonInvalid,
                            Error_desc = "Json payload is Invalid - Full Payload and Partial Payload cannot be passed simultaneously"
                        });
                        _objectsError.providerError = new ProviderError()
                        {
                            Id = null,
                            Errors = errorsModal
                        };
                        _logRepository.AddLogDetails(LogLevel.Error, SeverityLevels.FullCritical.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"{_jsonInvalid} - Json payload is Invalid - Full Payload and Partial Payload cannot be passed simultaneously");
                    }                   
                }
                else
                {
                    foreach (var failure in headerValidation.Errors)
                        providerValidationResult.Add($"{failure.PropertyName}: {failure.ErrorMessage}");
                    _logRepository.AddLogDetails(LogLevel.Error, SeverityLevels.FullCritical.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"Provider Header field values are not Valid {string.Join(",", providerValidationResult)}");
                    providerValidationResult.Clear();
                }
            }
            catch (JsonException)
            {
                _logRepository.AddLogDetails(LogLevel.Error, SeverityLevels.FullCritical.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"{_jsonInvalid} - Json payload is Invalid - All Json properties not passed in payload");
                errorsModal.Add(new ErrorsModal()
                {
                    Error_code = _jsonInvalid,
                    Error_desc = "Json payload is Invalid - All Json properties not passed in payload"
                });
                _objectsError.providerError = new ProviderError()
                {
                    Id = null,
                    Errors = errorsModal
                };
            }
            catch (System.Exception ex)
            {
                _logRepository.AddLogDetails(LogLevel.Error, SeverityLevels.FullCritical.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"System.Exception in GetProviderQuery: {ex.StackTrace}");
            }
            finally
            {
                Error error = new Error();
                error.ProviderError = _objectsError.providerError.Errors != null ? error.ProviderError = _objectsError.providerError : error.ProviderError = null;
                error.MemberProviderError = _objectsError.memberproviderError.Count > 0 ? error.MemberProviderError = _objectsError.memberproviderError : error.MemberProviderError = null;
                error.ProviderContractError = _objectsError.providercontractError.Count > 0 ? error.ProviderContractError = _objectsError.providercontractError : error.ProviderContractError = null;
                error.ProviderEmailAddressError = _objectsError.provideremailaddressError.Count > 0 ? error.ProviderEmailAddressError = _objectsError.provideremailaddressError : error.ProviderEmailAddressError = null;
                error.ProviderPhoneNumberError = _objectsError.providerphonenumberError.Count > 0 ? error.ProviderPhoneNumberError = _objectsError.providerphonenumberError : error.ProviderPhoneNumberError = null;
                error.ProviderPostalAddressError = _objectsError.providerpostaladdressError.Count > 0 ? error.ProviderPostalAddressError = _objectsError.providerpostaladdressError : error.ProviderPostalAddressError = null;
                error.ProviderIdentifierError = _objectsError.provideridentifierError.Count > 0 ? error.ProviderIdentifierError = _objectsError.provideridentifierError : error.ProviderIdentifierError = null;
                error.ProviderNetworkError = _objectsError.providernetworkError.Count > 0 ? error.ProviderNetworkError = _objectsError.providernetworkError : error.ProviderNetworkError = null;


                Success success = new Success();
                success.Provider = _objectsSuccess.providerSuccess.Ccx_id != null ? success.Provider = _objectsSuccess.providerSuccess : success.Provider = null;
                success.MemberProvider = (_objectsSuccess.memberproviderSuccess.Count > 0) ? success.MemberProvider = _objectsSuccess.memberproviderSuccess : success.MemberProvider = null;
                success.ProviderContract = (_objectsSuccess.providercontractSuccess.Count > 0) ? success.ProviderContract = _objectsSuccess.providercontractSuccess : success.ProviderContract = null;
                success.ProviderEmailAddress = (_objectsSuccess.provideremailaddressSuccess.Count > 0) ? success.ProviderEmailAddress = _objectsSuccess.provideremailaddressSuccess : success.ProviderEmailAddress = null;
                success.ProviderPhoneNumber = (_objectsSuccess.providerphoneNumberSuccess.Count > 0) ? success.ProviderPhoneNumber = _objectsSuccess.providerphoneNumberSuccess : success.ProviderPhoneNumber = null;
                success.ProviderPostalAddress = (_objectsSuccess.providerpostaladdressSuccess.Count > 0) ? success.ProviderPostalAddress = _objectsSuccess.providerpostaladdressSuccess : success.ProviderPostalAddress = null;
                success.ProviderIdentifier = (_objectsSuccess.provideridentifierSuccess.Count > 0) ? success.ProviderIdentifier = _objectsSuccess.provideridentifierSuccess : success.ProviderIdentifier = null;
                success.ProviderNetwork = (_objectsSuccess.providernetworkSuccess.Count > 0) ? success.ProviderNetwork = _objectsSuccess.providernetworkSuccess : success.ProviderNetwork = null;
                _resultData.Responses.Error = error;
                _resultData.Responses.Success = success;
                _logRepository.AddLogDetails(NLog.LogLevel.Info, SeverityLevels.Information.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"Provider Test - " + JsonConvert.SerializeObject(_resultData,
                    Newtonsoft.Json.Formatting.None,
                    new JsonSerializerSettings
                    {
                        NullValueHandling = NullValueHandling.Ignore
                    }));

                _objectsError.providerError.Errors.Clear();
                _objectsError.providerError.Id = null;
                _objectsError.memberproviderError.Clear();
                _objectsError.providercontractError.Clear();
                _objectsError.provideremailaddressError.Clear();
                _objectsError.providerphonenumberError.Clear();
                _objectsError.providerpostaladdressError.Clear();
                _objectsError.provideridentifierError.Clear();
                _objectsError.providernetworkError.Clear();
                _objectsSuccess.providerSuccess.Ccx_id = null;
                _objectsSuccess.providerSuccess.Mc_id = null;
                _objectsSuccess.providercontractSuccess.Clear();
                _objectsSuccess.provideremailaddressSuccess.Clear();
                _objectsSuccess.providerphoneNumberSuccess.Clear();
                _objectsSuccess.providerpostaladdressSuccess.Clear();
                _objectsSuccess.provideridentifierSuccess.Clear();
                _objectsSuccess.providernetworkSuccess.Clear();
                _objectsSuccess.memberproviderSuccess.Clear();
            }
            return validationResult;
        }

        #endregion

        #region ValidateAndSaveProvider
        private async Task<bool> ValidateAndSaveProvider(ProviderDetails providerdata)
        {
            List<string> payLoadErrors = new List<string>();
            bool result = false;
            try
            {
                errorsModal = new List<ErrorsModal>();
                var providervalidation = _providerValidator.Validate(providerdata.Provider);
                if (providervalidation.IsValid)
                {
                    var providerObj = await _getTableInfo.GetMedCompassDbData(providerdata.Provider.SourceProviderId, ProviderInfoQueryEnum.QueryProviderWithParameter.GetEnumDescription());
                    _providerId = providerObj?["ProviderId"].ToString();
                    if (string.IsNullOrEmpty(_providerId) && _processId == _eventTypeIdValueAdd || !string.IsNullOrEmpty(_providerId) && _processId == _eventTypeIdValueNotify)
                    {
                        var providerContents = providerdata.Provider;
                        _providerRiaModel.providerDemographic.ProviderId = string.IsNullOrEmpty(_providerId) ? Guid.NewGuid() : new Guid(_providerId);
                        _providerRiaModel.providerDemographic.ParentId = providerObj != null ? providerContents.ParentId.GuidValue(providerObj["ParentId"], _processId) : providerContents.ParentId.GuidValue();
                        _providerRiaModel.providerDemographic.ProviderTypeKey = providerObj != null ? providerContents.ProviderTypeKey.StringValue(providerObj["ProviderTypeKey"], _processId) : providerContents.ProviderTypeKey.StringValue();
                        _providerRiaModel.providerDemographic.OrganizationName = providerObj != null ? providerContents.OrganizationName.StringValue(providerObj["OrganizationName"], _processId) : providerContents.OrganizationName.StringValue();
                        _providerRiaModel.providerDemographic.ShortName = providerObj != null ? providerContents.ShortName.StringValue(providerObj["ShortName"], _processId) : providerContents.ShortName.StringValue();
                        _providerRiaModel.providerDemographic.FirstName = providerObj != null ? providerContents.FirstName.StringValue(providerObj["FirstName"], _processId) : providerContents.FirstName.StringValue();
                        _providerRiaModel.providerDemographic.LastName = providerObj != null ? providerContents.LastName.StringValue(providerObj["LastName"], _processId) : providerContents.LastName.StringValue();
                        _providerRiaModel.providerDemographic.MiddleName = providerObj != null ? providerContents.MiddleName.StringValue(providerObj["MiddleName"], _processId) : providerContents.MiddleName.StringValue();
                        _providerRiaModel.providerDemographic.GenderTypeKey = providerObj != null ? providerContents.GenderTypeKey.StringValue(providerObj["GenderTypeKey"], _processId) : providerContents.GenderTypeKey.StringValue();
                        _providerRiaModel.providerDemographic.EthnicGroupTypeKey = providerObj != null ? providerContents.EthnicGroupTypeKey.StringValue(providerObj["EthnicGroupTypeKey"], _processId) : providerContents.EthnicGroupTypeKey.StringValue();
                        _providerRiaModel.providerDemographic.ReligionTypeKey = providerObj != null ? providerContents.ReligionTypeKey.StringValue(providerObj["ReligionTypeKey"], _processId) : providerContents.ReligionTypeKey.StringValue();
                        _providerRiaModel.providerDemographic.AgeSeenFrom = providerObj != null ? providerContents.AgeSeenFrom.DecimalValueNotNull(providerObj["AgeSeenFrom"], _processId) : providerContents.AgeSeenFrom.DecimalValueNotNull();
                        _providerRiaModel.providerDemographic.AgeSeenTo = providerObj != null ? providerContents.AgeSeenTo.DecimalValueNotNull(providerObj["AgeSeenTo"], _processId) : providerContents.AgeSeenTo.DecimalValueNotNull();
                        _providerRiaModel.providerDemographic.DefaultIPAId = providerObj != null ? providerContents.DefaultIPAId.GuidValue(providerObj["DefaultIPAId"], _processId) : providerContents.DefaultIPAId.GuidValue();
                        _providerRiaModel.providerDemographic.Institutional = (providerContents.Institutional == "1") ? true : false;
                        _providerRiaModel.providerDemographic.PhysicianAdvisor = (providerContents.PhysicianAdvisor == "1") ? true : false;
                        _providerRiaModel.providerDemographic.Comments = providerObj != null ? providerContents.Comments.StringValue(providerObj["Comments"], _processId) : providerContents.Comments.StringValue();
                        _providerRiaModel.providerDemographic.Website = providerObj != null ? providerContents.Website.StringValue(providerObj["Website"], _processId) : providerContents.Website.StringValue();
                        _providerRiaModel.providerDemographic.LicenseNumber = providerObj != null ? providerContents.LicenseNumber.StringValue(providerObj["LicenseNumber"], _processId) : providerContents.LicenseNumber.StringValue();
                        _providerRiaModel.providerDemographic.LicenseAddressStateID = providerObj != null ? providerContents.LicenseAddressStateId.GuidValue(providerObj["LicenseAddressStateID"], _processId) : providerContents.LicenseAddressStateId.GuidValue();
                        _providerRiaModel.providerDemographic.BHIndicator = providerObj != null ? providerContents.BhIndicator.StringValue(providerObj["BHIndicator"], _processId) : providerContents.BhIndicator.StringValue();
                        _providerRiaModel.providerDemographic.ProviderOrgAffiliationTypeKey = providerObj != null ? providerContents.ProviderOrgAffiliationTypeKey.StringValue(providerObj["ProviderOrgAffiliationTypeKey"], _processId) : providerContents.ProviderOrgAffiliationTypeKey.StringValue();
                        _providerRiaModel.providerDemographic.EligibilityRequierments = providerObj != null ? providerContents.EligibilityRequirements.StringValue(providerObj["EligibilityRequierments"], _processId) : providerContents.EligibilityRequirements.StringValue();
                        _providerRiaModel.providerDemographic.ExternalProvider = (providerContents.ExternalProvider == "1") ? true : false;
                        _providerRiaModel.providerDemographic.IOEIndicator = providerObj != null ? providerContents.IOEIndicator.StringValue(providerObj["IOEIndicator"], _processId) : providerContents.IOEIndicator.StringValue();
                        _providerRiaModel.providerDemographic.GoldCard = providerObj != null ? providerContents.GoldCard.StringValue(providerObj["GoldCard"], _processId) : providerContents.GoldCard.StringValue();
                        _providerRiaModel.providerDemographic.ConversionId = providerContents.SourceProviderId;
                        _providerRiaModel.providerDemographic.ProviderBoardStatusTypeKey = providerObj != null ? providerContents.ProviderBoardStatusTypeKey.StringValue(providerObj["ProviderBoardStatusTypeKey"], _processId) : providerContents.ProviderBoardStatusTypeKey.StringValue();
                        _providerRiaModel.providerDemographic.TimeZoneTypeKey = providerObj != null ? providerContents.TimeZoneTypeKey.StringValue(providerObj["TimeZoneTypeKey"], _processId) : providerContents.TimeZoneTypeKey.StringValue();
                        _providerRiaModel.providerDemographic.ExpirationDate = providerObj != null ? providerContents.ExpirationDate.DateTimeNullable(providerObj["ExpirationDate"], _processId) : providerContents.ExpirationDate.DateTimeNullable();
                        _providerRiaModel.providerDemographic.EffectiveDate = providerObj != null ? providerContents.EffectiveDate.DateTimeNotNull(providerObj["EffectiveDate"], _processId) : providerContents.EffectiveDate.DateTimeNotNull();

                        var changeSets = new List<Ria.ChangeSetEntry>();
                        if (string.IsNullOrEmpty(_providerId))
                            changeSets.Add(new ChangeSetEntry { Id = 1, Entity = _providerRiaModel.providerDemographic, Operation = DomainOperation.Insert });
                        else
                            changeSets.Add(new ChangeSetEntry { Id = 1, Entity = _providerRiaModel.providerDemographic, Operation = DomainOperation.Update });
                        var response = await _mCServices.RiaForProviderSubmitChanges(changeSets);
                        response[0].ValidationErrors.ForEach(x => { payLoadErrors.Add($"{x.ErrorCode}- {x.Message}"); });
                        response[0].ValidationErrors.ForEach(x =>
                        {
                            errorsModal.Add(new ErrorsModal()
                            {
                                Error_code = x.ErrorCode.ToString(),
                                Error_desc = x.Message
                            });
                        });
                        changeSets.Clear();
                    }
                    else
                    {
                        if (_processId == _eventTypeIdValueAdd)
                        {
                            errorsModal.Add(new ErrorsModal()
                            {
                                Error_code = _eventTypeIdValueError,
                                Error_desc = "Provider not added(sourceproviderid:" + providerdata.Provider.SourceProviderId + " Previously Added)"
                            });
                            payLoadErrors.Add($"{_eventTypeIdValueError} - Provider not added (sourceproviderid:{providerdata.Provider.SourceProviderId} Previously Added)");
                        }
                        else
                        {
                            payLoadErrors.Add($"{_ParentProvider} - Provider cannot be found(sourceproviderid:{providerdata.Provider.SourceProviderId})");
                            errorsModal.Add(new ErrorsModal()
                            {
                                Error_code = _ParentProvider,
                                Error_desc = "Provider cannot be found(sourceproviderid: " + providerdata.Provider.SourceProviderId + ")"
                            });
                        }
                    }
                }
                else
                {
                    foreach (var failure in providervalidation.Errors)
                    {
                        errorsModal.Add(new ErrorsModal()
                        {
                            Error_code = failure.ErrorCode,
                            Error_desc = failure.PropertyName + " " + failure.ErrorMessage
                        });
                        payLoadErrors.Add($"{ failure.ErrorCode} - {failure.PropertyName} {failure.ErrorMessage}");
                    }
                }
                if (payLoadErrors.Count > 0)
                {
                    _logRepository.AddLogDetails(LogLevel.Info, SeverityLevels.Information.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"Ineligible Provider payload object - {JsonConvert.SerializeObject(providerdata.Provider)}");
                    _logRepository.AddLogDetails(LogLevel.Error, SeverityLevels.FullCritical.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"Provider object not a valid payload : {string.Join(",", payLoadErrors)}");
                    _objectsError.providerError = new ProviderError()
                    {
                        Id = providerdata.Provider.SourceProviderId,
                        Errors = errorsModal
                    };
                }
                else
                {
                    var dbvalue = await _getTableInfo.GetMedCompassDbData(providerdata.Provider.SourceProviderId, ProviderInfoQueryEnum.QueryProviderWithParameter.GetEnumDescription());
                    _objectsSuccess.providerSuccess = new ProviderSuccess()
                    {
                        Ccx_id = providerdata.Provider.SourceProviderId,
                        Mc_id = dbvalue[0].ToString()
                    };
                    _logRepository.AddLogDetails(LogLevel.Info, SeverityLevels.Information.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"Provider object successfully stored into MedCompass DB for SourceProviderId:{providerdata.Provider.SourceProviderId}");
                }
                result = payLoadErrors.Count > 0 ? false : true;
                payLoadErrors.Clear();
            }
            catch (System.Exception ex)
            {
                _logRepository.AddLogDetails(LogLevel.Error, SeverityLevels.FullCritical.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"Exception in Validation and Insertion of Provider object {ex.StackTrace}");
            }
            return result;
        }
        #endregion

        #region ValidateAndSaveMemberProvider
        private async Task<bool> ValidateAndSaveMemberProvider(List<Assure.MemberProvider> memberProvider)
        {
            List<string> payLoadErrors = new List<string>();
            bool result = false;
            try
            {
                foreach (var data in memberProvider)
                {
                    errorsModal = new List<ErrorsModal>();
                    var validationResult = _pmemberProviderValidator.Validate(data);
                    if (validationResult.IsValid)
                    {
                        if (string.IsNullOrEmpty(_providerId))
                        {
                            var providerObj = await _getTableInfo.GetMedCompassDbData(data.SourceProviderId, ProviderInfoQueryEnum.QueryProviderWithParameter.GetEnumDescription());
                            _providerId = providerObj?["ProviderId"].ToString();
                        }
                        var mCMemberId = await _getTableInfo.GetMedCompassDbData(data.SourceMemberId, ProviderInfoQueryEnum.QueryMemberWithParameterForMemberId.GetEnumDescription());
                        if (!string.IsNullOrEmpty(_providerId) && !string.IsNullOrEmpty(mCMemberId[0].ToString()))
                        {
                            var memberProviderObj = await _getTableInfo.GetMedCompassDbData(data.SourceMemberProviderId, ProviderInfoQueryEnum.QueryMemberProviderWithParameter.GetEnumDescription());
                            var mCMemberProviderId = memberProviderObj?["MemberProviderId"].ToString();
                            if (string.IsNullOrEmpty(mCMemberProviderId) && _processId == _eventTypeIdValueAdd || !string.IsNullOrEmpty(mCMemberProviderId) && _processId == _eventTypeIdValueNotify)
                            {
                                var mCProviderPostalAddressId = string.IsNullOrEmpty(data.SourceProviderPostalAddressId) ? null : await _getTableInfo.GetMedCompassDbData(data.SourceProviderPostalAddressId, ProviderInfoQueryEnum.QueryProviderPostalAddressWithParameter.GetEnumDescription());
                                var providerPostalAddressId = mCProviderPostalAddressId?["ProviderPostalAddressId"].ToString();
                                var mCMemberProgramId = string.IsNullOrEmpty(data.SourceMemberProgramId) ? null : await _getTableInfo.GetMedCompassDbData(data.SourceMemberProgramId, ProviderInfoQueryEnum.QueryMemberWithParameterForMemberProgrameId.GetEnumDescription());
                                var memberProgramId = mCMemberProgramId?["MemberProgramID"].ToString();
                                _providerRiaModel.memberProvider.MemberProviderId = string.IsNullOrEmpty(mCMemberProviderId) ? Guid.NewGuid() : new Guid(mCMemberProviderId);
                                _providerRiaModel.memberProvider.MemberId = new Guid(mCMemberId[0].ToString());
                                _providerRiaModel.memberProvider.ProviderId = new Guid(_providerId);
                                _providerRiaModel.memberProvider.PrimaryCareFlag = (data.PrimaryCareFlag == "1") ? true : false;
                                _providerRiaModel.memberProvider.OBGYNFlag = (data.ObgynFlag == "1") ? true : false;
                                _providerRiaModel.memberProvider.StartDate = DateTime.Parse(data.StartDate);
                                _providerRiaModel.memberProvider.EndDate = memberProviderObj != null ? data.EndDate.DateTimeNullable(memberProviderObj["EndDate"], _processId) : data.EndDate.DateTimeNullable();
                                _providerRiaModel.memberProvider.ExpirationDate = memberProviderObj != null ? data.ExpirationDate.DateTimeNullable(memberProviderObj["ExpirationDate"], _processId) : data.ExpirationDate.DateTimeNullable();
                                _providerRiaModel.memberProvider.EffectiveDate = memberProviderObj != null ? data.EffectiveDate.DateTimeNotNull(memberProviderObj["EffectiveDate"], _processId) : data.EffectiveDate.DateTimeNotNull();
                                _providerRiaModel.memberProvider.ProgramTypeKey = memberProviderObj != null ? data.ProgramTypeKey.StringValue(memberProviderObj["ProgramTypeKey"], _processId) : data.ProgramTypeKey.StringValue();
                                _providerRiaModel.memberProvider.ConversionId = data.SourceMemberProviderId;
                                _providerRiaModel.memberProvider.ProviderPostalAddressId = !string.IsNullOrEmpty(providerPostalAddressId) ? new Guid(providerPostalAddressId) : (Guid?)null;
                                _providerRiaModel.memberProvider.MemberProgramID = !string.IsNullOrEmpty(memberProgramId) ? new Guid(memberProgramId) : (Guid?)null;

                                var changeSets = new List<Ria.ChangeSetEntry>();
                                if (string.IsNullOrEmpty(mCMemberProviderId))
                                    changeSets.Add(new ChangeSetEntry { Id = 1, Entity = _providerRiaModel.memberProvider, Operation = DomainOperation.Insert });
                                else
                                    changeSets.Add(new ChangeSetEntry { Id = 1, Entity = _providerRiaModel.memberProvider, Operation = DomainOperation.Update });
                                var response = await _mCServices.RiaForMemberSubmitChanges(changeSets);
                                response[0].ValidationErrors.ForEach(x => { payLoadErrors.Add($"{x.ErrorCode}- {x.Message}"); });
                                response[0].ValidationErrors.ForEach(x =>
                                {
                                    errorsModal.Add(new ErrorsModal()
                                    {
                                        Error_code = x.ErrorCode.ToString(),
                                        Error_desc = x.Message
                                    }); ;
                                });
                                changeSets.Clear();
                            }
                            else
                            {
                                if (_processId == _eventTypeIdValueAdd)
                                {
                                    payLoadErrors.Add($"{_eventTypeIdValueError} - MemberProvider not added (sourcememberproviderid:{data.SourceMemberProviderId} previously added)");
                                    errorsModal.Add(new ErrorsModal()
                                    {
                                        Error_code = _eventTypeIdValueError,
                                        Error_desc = "MemberProvider not added(sourcememberproviderid:" + data.SourceMemberProviderId + " previously added)"
                                    }); ;
                                }
                                else
                                {
                                    errorsModal.Add(new ErrorsModal()
                                    {
                                        Error_code = _ParentProvider,
                                        Error_desc = "MemberProvider cannot be found (sourcememberproviderid: " + data.SourceMemberProviderId + ")"
                                    });
                                    payLoadErrors.Add($"{_ParentProvider} - MemberProvider cannot be found (sourcememberproviderid:{data.SourceMemberProviderId})");
                                }
                            }
                        }
                        else
                        {
                            if (string.IsNullOrEmpty(mCMemberId[0].ToString()))
                            {
                                errorsModal.Add(new ErrorsModal()
                                {
                                    Error_code = _ParentMember,
                                    Error_desc = "Member cannot be found (sourcememberproviderid: " + data.SourceMemberId + ")"
                                });
                                payLoadErrors.Add($"{_ParentMember} - Member cannot be found(sourcememberid:{data.SourceMemberId})");
                            }
                            if (string.IsNullOrEmpty(_providerId))
                            {
                                errorsModal.Add(new ErrorsModal()
                                {
                                    Error_code = _ParentProvider,
                                    Error_desc = "Provider cannot be found(sourceproviderid " + data.SourceProviderId + ")"
                                });
                                payLoadErrors.Add($"{_ParentProvider} - Provider cannot be found(sourceproviderid:{data.SourceProviderId})");
                            }
                        }
                    }
                    else
                    {
                        foreach (var failure in validationResult.Errors)
                        {
                            errorsModal.Add(new ErrorsModal()
                            {
                                Error_code = failure.ErrorCode,
                                Error_desc = failure.PropertyName + " " + failure.ErrorMessage
                            });
                            payLoadErrors.Add($"{failure.ErrorCode} - {failure.PropertyName} {failure.ErrorMessage}");
                        }
                    }
                    if (payLoadErrors.Count > 0)
                    {
                        _logRepository.AddLogDetails(LogLevel.Info, SeverityLevels.Information.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"Ineligible MemberProvider payload object - {JsonConvert.SerializeObject(data)}");
                        _logRepository.AddLogDetails(LogLevel.Error, SeverityLevels.PartialCritical.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"MemberProvider object not a valid payload: {string.Join(",", payLoadErrors)}");
                        _objectsError.memberproviderError.Add(new MemberProviderError()
                        {
                            Id = data.SourceMemberProviderId,
                            Errors = errorsModal
                        });
                    }
                    else
                    {
                        var dbvalue = await _getTableInfo.GetMedCompassDbData(data.SourceMemberProviderId, ProviderInfoQueryEnum.QueryMemberProviderWithParameter.GetEnumDescription());
                        _objectsSuccess.memberproviderSuccess.Add(new MemberProviderSuccess()
                        {
                            Ccx_id = data.SourceMemberProviderId,
                            Mc_id = dbvalue[0].ToString()
                        });
                        _logRepository.AddLogDetails(LogLevel.Info, SeverityLevels.Information.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"MemberProvider object object successfully stored into MedCompass DB for SourceMemberProviderId:{data.SourceMemberProviderId}");
                    }
                    
                    result = payLoadErrors.Count > 0 ? false : true;
                    payLoadErrors.Clear();
                }
            }
            catch (System.Exception ex)
            {
                _logRepository.AddLogDetails(LogLevel.Error, SeverityLevels.PartialCritical.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"System.Exception in Validation and Insertion of MemberProvider object {ex.StackTrace}");
            }
            return result;
        }
        #endregion

        #region ValidateAndSaveProviderContract
        private async Task<bool> ValidateAndSaveProviderContract(List<Assure.ProviderContract> providerContract)
        {
            List<string> payLoadErrors = new List<string>();
            bool result = false;
            try
            {
                foreach (var data in providerContract)
                {
                    errorsModal = new List<ErrorsModal>();
                    var validationResult = _pContractValidator.Validate(data);
                    if (validationResult.IsValid)
                    {
                       
                        if (string.IsNullOrEmpty(_providerId))
                        {
                            var providerObj = await _getTableInfo.GetMedCompassDbData(data.SourceProviderId, ProviderInfoQueryEnum.QueryProviderWithParameter.GetEnumDescription());
                            _providerId = providerObj?["ProviderId"].ToString();
                        }
                        if (!string.IsNullOrEmpty(_providerId))
                        {
                            var providerContractObj = await _getTableInfo.GetMedCompassDbData(data.SourceProviderContractId, ProviderInfoQueryEnum.QueryProviderContractWithParameter.GetEnumDescription());
                            var mCProviderContractId = providerContractObj?["ProviderContractId"].ToString();
                            if (string.IsNullOrEmpty(mCProviderContractId) && _processId == _eventTypeIdValueAdd || !string.IsNullOrEmpty(mCProviderContractId) && _processId == _eventTypeIdValueNotify)
                            {
                                _providerRiaModel.providerContract.ProviderContractId = string.IsNullOrEmpty(mCProviderContractId) ? Guid.NewGuid() : new Guid(mCProviderContractId);
                                _providerRiaModel.providerContract.ProviderId = new Guid(_providerId);
                                _providerRiaModel.providerContract.LineOfBusinessTypeKey = data.LineOfBusinessTypeKey;
                                _providerRiaModel.providerContract.TermReason = providerContractObj != null ? data.TermReason.StringValue(providerContractObj["TermReason"], _processId) : data.TermReason.StringValue();
                                _providerRiaModel.providerContract.ParticipationStatusTypeKey = providerContractObj != null ? data.ParticipationStatusTypeKey.StringValue(providerContractObj["ParticipationStatusTypeKey"], _processId) : data.ParticipationStatusTypeKey.StringValue();
                                _providerRiaModel.providerContract.ContractTypeDescription = providerContractObj != null ? data.ContractTypeDescription.StringValue(providerContractObj["ContractTypeDescription"], _processId) : data.ContractTypeDescription.StringValue();
                                _providerRiaModel.providerContract.FedTaxIdentifier = providerContractObj != null ? data.FedTaxIdentifier.StringValue(providerContractObj["FedTaxIdentifier"], _processId) : data.FedTaxIdentifier.StringValue();
                                _providerRiaModel.providerContract.ConversionId = data.SourceProviderContractId;
                                _providerRiaModel.providerContract.PCP = (data.PCP == "1") ? true : false;
                                _providerRiaModel.providerContract.AcceptNewPatient = (data.AcceptNewPatient == "1") ? true : false;
                                _providerRiaModel.providerContract.IPAId = providerContractObj != null ? data.IpaId.GuidValue(providerContractObj["IPAId"], _processId) : data.IpaId.GuidValue();
                                _providerRiaModel.providerContract.ExpirationDate = providerContractObj != null ? data.ExpirationDate.DateTimeNullable(providerContractObj["ExpirationDate"], _processId) : data.ExpirationDate.DateTimeNullable();
                                _providerRiaModel.providerContract.EffectiveDate = providerContractObj != null ? data.EffectiveDate.DateTimeNotNull(providerContractObj["EffectiveDate"], _processId) : data.EffectiveDate.DateTimeNotNull();

                                var changeSets = new List<ChangeSetEntry>();
                                if (string.IsNullOrEmpty(mCProviderContractId))
                                    changeSets.Add(new ChangeSetEntry { Id = 1, Entity = _providerRiaModel.providerContract, Operation = DomainOperation.Insert });
                                else
                                    changeSets.Add(new ChangeSetEntry { Id = 1, Entity = _providerRiaModel.providerContract, Operation = DomainOperation.Update });
                                var response = await _mCServices.RiaForProviderSubmitChanges(changeSets);
                                response[0].ValidationErrors.ForEach(x => { payLoadErrors.Add($"{x.ErrorCode}- {x.Message}"); });
                                response[0].ValidationErrors.ForEach(x =>
                                {
                                    errorsModal.Add(new ErrorsModal()
                                    {
                                        Error_code = x.ErrorCode.ToString(),
                                        Error_desc = x.Message
                                    });
                                });
                                changeSets.Clear();
                            }
                            else
                            {
                                if (_processId == _eventTypeIdValueAdd)
                                {
                                    payLoadErrors.Add($"{_eventTypeIdValueError} - ProviderContract not added (sourceprovidercontractid:{data.SourceProviderContractId} previously added)");
                                    errorsModal.Add(new ErrorsModal()
                                    {
                                        Error_code = _eventTypeIdValueError,
                                        Error_desc = "ProviderContract not added (sourceprovidercontractid:" + data.SourceProviderContractId + " previously added)"
                                    });
                                }
                                else
                                {
                                    errorsModal.Add(new ErrorsModal()
                                    {
                                        Error_code = _ParentProvider,
                                        Error_desc = " providerContract cannot be found(sourceprovidercontractid " + data.SourceProviderContractId + ")"
                                    });
                                    payLoadErrors.Add($"{_ParentProvider} - providerContract cannot be found(sourceprovidercontractid:{data.SourceProviderContractId})");
                                }
                            }
                        }
                        else
                        {
                            errorsModal.Add(new ErrorsModal()
                            {
                                Error_code = _ParentProvider,
                                Error_desc = "Provider cannot be found(sourceproviderid " + data.SourceProviderId + ")"
                            });
                            payLoadErrors.Add($"{_ParentProvider} - Provider cannot be found(sourceproviderid:{data.SourceProviderId})");
                        }
                    }
                    else
                    {
                        foreach (var failure in validationResult.Errors)
                        {
                            errorsModal.Add(new ErrorsModal()
                            {
                                Error_code = failure.ErrorCode,
                                Error_desc = failure.PropertyName + " " + failure.ErrorMessage
                            });
                            payLoadErrors.Add($"{failure.ErrorCode} - {failure.PropertyName} {failure.ErrorMessage}");
                        }
                    }
                    if (payLoadErrors.Count > 0)
                    {
                        _logRepository.AddLogDetails(LogLevel.Info, SeverityLevels.Information.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"Ineligible ProviderContract payload object - {JsonConvert.SerializeObject(data)}");
                        _logRepository.AddLogDetails(LogLevel.Error, SeverityLevels.PartialCritical.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"ProviderContract object not a valid payload : {string.Join(",", payLoadErrors)}");
                        _objectsError.providercontractError.Add(new ProviderContractError()
                        {
                            Id = data.SourceProviderContractId,
                            Errors = errorsModal
                        });
                    }
                    else
                    {
                        var dbvalue = await _getTableInfo.GetMedCompassDbData(data.SourceProviderContractId, ProviderInfoQueryEnum.QueryProviderContractWithParameter.GetEnumDescription());
                        _objectsSuccess.providercontractSuccess.Add(new ProviderContractSuccess()
                        {
                            Ccx_id = data.SourceProviderContractId,
                            Mc_id = dbvalue[0].ToString()
                        });
                        _logRepository.AddLogDetails(LogLevel.Info, SeverityLevels.Information.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"ProviderContract object successfully stored into MedCompass DB for SourceProviderContractId:{data.SourceProviderContractId}");
                    }
                    
                    result = payLoadErrors.Count > 0 ? false : true;
                    payLoadErrors.Clear();
                }
            }
            catch (System.Exception ex)
            {
                _logRepository.AddLogDetails(LogLevel.Error, SeverityLevels.PartialCritical.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"System.Exception in Validation and Insertion of ProviderContract object {ex.StackTrace}");
            }
            return result;
        }
        #endregion

        #region ValidateAndSaveProviderEmailAddress
        private async Task<bool> ValidateAndSaveProviderEmailAddress(List<Assure.ProviderEmailAddress> providerEmailAddress)
        {
            bool result = false;
            List<string> payLoadErrors = new List<string>();
            try
            {
                foreach (var data in providerEmailAddress)
                {
                    errorsModal = new List<ErrorsModal>();
                    var validationResult = _providerEmailAddressValidator.Validate(data);
                    if (validationResult.IsValid)
                    {
                        if (string.IsNullOrEmpty(_providerId))
                        {
                            var providerObj = await _getTableInfo.GetMedCompassDbData(data.SourceProviderId, ProviderInfoQueryEnum.QueryProviderWithParameter.GetEnumDescription());
                            _providerId = providerObj?["ProviderId"].ToString();
                        }
                        if (!string.IsNullOrEmpty(_providerId))
                        {
                            var providerEmailAddressObj = await _getTableInfo.GetMedCompassDbData(data.SourceProviderEmailAddressId, ProviderInfoQueryEnum.QueryProviderEmailAddressWithParameter.GetEnumDescription());
                            var mCProviderEmailAddressId = providerEmailAddressObj?["ProviderEmailAddressId"].ToString();
                            if (string.IsNullOrEmpty(mCProviderEmailAddressId) && _processId == _eventTypeIdValueAdd || !string.IsNullOrEmpty(mCProviderEmailAddressId) && _processId == _eventTypeIdValueNotify)
                            {
                                _providerRiaModel.providerEmailAddress.ProviderEmailAddressId = string.IsNullOrEmpty(mCProviderEmailAddressId) ? Guid.NewGuid() : new Guid(mCProviderEmailAddressId);
                                _providerRiaModel.providerEmailAddress.ProviderId = new Guid(_providerId);
                                _providerRiaModel.providerEmailAddress.Email = data.Email;
                                _providerRiaModel.providerEmailAddress.EmailAddressTypeKey = data.EmailAddressTypeKey;
                                _providerRiaModel.providerEmailAddress.PrimaryEmail = (data.PrimaryEmail == "1") ? true : false;
                                _providerRiaModel.providerEmailAddress.ExpirationDate = providerEmailAddressObj != null ? data.ExpirationDate.DateTimeNullable(providerEmailAddressObj["ExpirationDate"], _processId) : data.ExpirationDate.DateTimeNullable();
                                _providerRiaModel.providerEmailAddress.EffectiveDate = providerEmailAddressObj != null ? data.EffectiveDate.DateTimeNotNull(providerEmailAddressObj["EffectiveDate"], _processId) : data.EffectiveDate.DateTimeNotNull();
                                _providerRiaModel.providerEmailAddress.ConversionId = data.SourceProviderEmailAddressId;
                                var changeSets = new List<ChangeSetEntry>();
                                if (string.IsNullOrEmpty(mCProviderEmailAddressId))
                                    changeSets.Add(new ChangeSetEntry { Id = 1, Entity = _providerRiaModel.providerEmailAddress, Operation = DomainOperation.Insert });
                                else
                                    changeSets.Add(new ChangeSetEntry { Id = 1, Entity = _providerRiaModel.providerEmailAddress, Operation = DomainOperation.Update });
                                var response = await _mCServices.RiaForProviderSubmitChanges(changeSets);
                                response[0].ValidationErrors.ForEach(x => { payLoadErrors.Add($"{x.ErrorCode}- {x.Message}"); });
                                response[0].ValidationErrors.ForEach(x =>
                                {
                                    errorsModal.Add(new ErrorsModal()
                                    {
                                        Error_code = x.ErrorCode.ToString(),
                                        Error_desc = x.Message
                                    });
                                });
                                changeSets.Clear();
                            }
                            else
                            {
                                if (_processId == _eventTypeIdValueAdd)
                                {
                                    payLoadErrors.Add($"{_eventTypeIdValueError} - ProviderEmailAddress not added (sourceprovideremailaddressid:{data.SourceProviderEmailAddressId} previously added)");
                                    errorsModal.Add(new ErrorsModal()
                                    {
                                        Error_code = _eventTypeIdValueError,
                                        Error_desc = "ProviderEmailAddress not added (sourceprovideremailaddressid:" + data.SourceProviderEmailAddressId + " previously added)"
                                    });
                                }
                                else
                                {
                                    errorsModal.Add(new ErrorsModal()
                                    {
                                        Error_code = _ParentProvider,
                                        Error_desc = "ProviderEmailAddress cannot be found(sourceprovideremailaddressid:" + data.SourceProviderEmailAddressId + ")"
                                    });
                                    payLoadErrors.Add($"{_ParentProvider} - ProviderEmailAddress cannot be found(sourceprovideremailaddressid:{data.SourceProviderEmailAddressId})");
                                }
                            }
                        }
                        else
                        {
                            errorsModal.Add(new ErrorsModal()
                            {
                                Error_code = _ParentProvider,
                                Error_desc = "Provider cannot be found(sourceproviderid " + data.SourceProviderId + ")"
                            });
                            payLoadErrors.Add($"{_ParentProvider} - Provider cannot be found(sourceproviderid:{data.SourceProviderId})");
                        }
                    }
                    else
                    {
                        foreach (var failure in validationResult.Errors)
                        {
                            errorsModal.Add(new ErrorsModal()
                            {
                                Error_code = failure.ErrorCode,
                                Error_desc = failure.PropertyName + " " + failure.ErrorMessage
                            });
                            payLoadErrors.Add($"{failure.ErrorCode} - {failure.PropertyName} {failure.ErrorMessage}");
                        }
                    }
                    if (payLoadErrors.Count > 0)
                    {
                        _logRepository.AddLogDetails(LogLevel.Info, SeverityLevels.Information.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"Ineligible ProviderEmailAddress payload object - {JsonConvert.SerializeObject(data)}");
                        _logRepository.AddLogDetails(LogLevel.Error, SeverityLevels.PartialCritical.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"ProviderEmailAddress object not a valid payload: {string.Join(",", payLoadErrors)}");
                        _objectsError.provideremailaddressError.Add(new ProviderEmailAddressError()
                        {
                            Id = data.SourceProviderEmailAddressId,
                            Errors = errorsModal
                        });
                    }
                    else
                    {
                        var dbvalue = await _getTableInfo.GetMedCompassDbData(data.SourceProviderEmailAddressId, ProviderInfoQueryEnum.QueryProviderEmailAddressWithParameter.GetEnumDescription());
                        _objectsSuccess.provideremailaddressSuccess.Add(new ProviderEmailAddressSuccess()
                        {
                            Ccx_id = data.SourceProviderEmailAddressId,
                            Mc_id = dbvalue[0].ToString()
                        });
                        _logRepository.AddLogDetails(LogLevel.Info, SeverityLevels.Information.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"providerEmailAddress object successfully stored into MedCompass DB for SourceProviderEmailAddressId : {data.SourceProviderEmailAddressId}");
                    }
                   
                    result = payLoadErrors.Count == 0 ? false : true;
                    payLoadErrors.Clear();
                }
            }
            catch (System.Exception ex)
            {
                _logRepository.AddLogDetails(LogLevel.Error, SeverityLevels.PartialCritical.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"System.Exception in Validation and Insertion of ProviderEmailAddress object {ex.StackTrace}");
            }
            return result;
        }
        #endregion

        #region ValidateAndSaveProviderPhoneNumber
        private async Task<bool> ValidateAndSaveProviderPhoneNumber(List<Assure.ProviderPhoneNumber> providerPhoneNumber)
        {
            bool result = false;
            List<string> payLoadErrors = new List<string>();
            try
            {
                foreach (var data in providerPhoneNumber)
                {
                    errorsModal = new List<ErrorsModal>();
                    var validationResult = _providerPhoneNumberValidator.Validate(data);
                    if (validationResult.IsValid)
                    {
                        if (string.IsNullOrEmpty(_providerId))
                        {
                            var providerObj = await _getTableInfo.GetMedCompassDbData(data.SourceProviderId, ProviderInfoQueryEnum.QueryProviderWithParameter.GetEnumDescription());
                            _providerId = providerObj?["ProviderId"].ToString();
                        }
                        if (!string.IsNullOrEmpty(_providerId))
                        {
                            var providerPhoneNumberObj = await _getTableInfo.GetMedCompassDbData(data.SourceProviderPhoneNumberid, ProviderInfoQueryEnum.QueryProviderPhoneNumberWithParameter.GetEnumDescription());
                            var mCProviderPhoneNumberId = providerPhoneNumberObj?["ProviderPhoneNumberId"].ToString();
                            if (string.IsNullOrEmpty(mCProviderPhoneNumberId) && _processId == _eventTypeIdValueAdd || !string.IsNullOrEmpty(mCProviderPhoneNumberId) && _processId == _eventTypeIdValueNotify)
                            {
                                _providerRiaModel.providerPhoneNumber.ProviderPhoneNumberId = string.IsNullOrEmpty(mCProviderPhoneNumberId) ? Guid.NewGuid() : new Guid(mCProviderPhoneNumberId);
                                _providerRiaModel.providerPhoneNumber.ProviderId = new Guid(_providerId);
                                _providerRiaModel.providerPhoneNumber.PhoneNumber = data.PhoneNumber;
                                _providerRiaModel.providerPhoneNumber.PhoneExtension = providerPhoneNumberObj != null ? data.PhoneExtension.StringValue(providerPhoneNumberObj["PhoneExtension"], _processId) : data.PhoneExtension.StringValue();
                                _providerRiaModel.providerPhoneNumber.ReversePhoneNumber = providerPhoneNumberObj != null ? data.ReversePhoneNumber.StringValue(providerPhoneNumberObj["ReversePhoneNumber"], _processId) : data.ReversePhoneNumber.StringValue();
                                _providerRiaModel.providerPhoneNumber.PhoneNumberTypeKey = data.PhoneNumberTypeKey;
                                _providerRiaModel.providerPhoneNumber.PrimaryPhone = (data.primaryphone == "1") ? true : false;
                                _providerRiaModel.providerPhoneNumber.ExpirationDate = providerPhoneNumberObj != null ? data.ExpirationDate.DateTimeNullable(providerPhoneNumberObj["ExpirationDate"], _processId) : data.ExpirationDate.DateTimeNullable();
                                _providerRiaModel.providerPhoneNumber.EffectiveDate = providerPhoneNumberObj != null ? data.EffectiveDate.DateTimeNotNull(providerPhoneNumberObj["EffectiveDate"], _processId) : data.EffectiveDate.DateTimeNotNull();
                                _providerRiaModel.providerPhoneNumber.ConversionId = data.SourceProviderPhoneNumberid;

                                var changeSets = new List<ChangeSetEntry>();
                                if (string.IsNullOrEmpty(mCProviderPhoneNumberId))
                                    changeSets.Add(new ChangeSetEntry { Id = 1, Entity = _providerRiaModel.providerPhoneNumber, Operation = DomainOperation.Insert });
                                else
                                    changeSets.Add(new ChangeSetEntry { Id = 1, Entity = _providerRiaModel.providerPhoneNumber, Operation = DomainOperation.Update });
                                var response = await _mCServices.RiaForProviderSubmitChanges(changeSets);
                                response[0].ValidationErrors.ForEach(x => { payLoadErrors.Add($"{x.ErrorCode}- {x.Message}"); });
                                response[0].ValidationErrors.ForEach(x =>
                                {
                                    errorsModal.Add(new ErrorsModal()
                                    {
                                        Error_code = x.ErrorCode.ToString(),
                                        Error_desc = x.Message
                                    });
                                });
                                changeSets.Clear();
                            }
                            else
                            {
                                if (_processId == _eventTypeIdValueAdd)
                                {
                                    payLoadErrors.Add($"{_eventTypeIdValueError} - ProviderPhoneNumber not added (sourceproviderphonenumberid:{data.SourceProviderPhoneNumberid} previously added)");
                                    errorsModal.Add(new ErrorsModal()
                                    {
                                        Error_code = _eventTypeIdValueError,
                                        Error_desc = "ProviderPhoneNumber not added(sourceproviderphonenumberid:" + data.SourceProviderPhoneNumberid + " previously added)"
                                    });
                                }
                                else
                                {
                                    errorsModal.Add(new ErrorsModal()
                                    {
                                        Error_code = _ParentProvider,
                                        Error_desc = "ProviderPhoneNumber cannot be found (sourceproviderphonenumberid: " + data.SourceProviderPhoneNumberid + ")"
                                    });
                                    payLoadErrors.Add($"{_ParentProvider} - ProviderPhoneNumber cannot be found(sourceproviderphonenumberid:{data.SourceProviderPhoneNumberid})");
                                }
                            }
                        }
                        else
                        {
                            errorsModal.Add(new ErrorsModal()
                            {
                                Error_code = _ParentProvider,
                                Error_desc = "Provider cannot be found(sourceproviderid " + data.SourceProviderId + ")"
                            });
                            payLoadErrors.Add($"{_ParentProvider} - Provider cannot be found(sourceproviderid:{data.SourceProviderId})");
                        }
                    }
                    else
                    {
                        foreach (var failure in validationResult.Errors)
                        {
                            errorsModal.Add(new ErrorsModal()
                            {
                                Error_code = failure.ErrorCode,
                                Error_desc = failure.PropertyName + " " + failure.ErrorMessage
                            });

                            payLoadErrors.Add($"{failure.ErrorCode} - {failure.PropertyName} {failure.ErrorMessage}");
                        }
                    }
                    if (payLoadErrors.Count > 0)
                    {
                        _logRepository.AddLogDetails(LogLevel.Info, SeverityLevels.Information.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"Ineligible ProviderPhoneNumber payload object - {JsonConvert.SerializeObject(data)}");
                        _logRepository.AddLogDetails(LogLevel.Error, SeverityLevels.PartialCritical.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"ProviderPhoneNumber object not a valid payload : {string.Join(",", payLoadErrors)}");
                        _objectsError.providerphonenumberError.Add(new ProviderPhoneNumberError()
                        {
                            Id = data.SourceProviderPhoneNumberid,
                            Errors = errorsModal
                        });
                    }
                    else
                    {
                        var dbvalue = await _getTableInfo.GetMedCompassDbData(data.SourceProviderPhoneNumberid, ProviderInfoQueryEnum.QueryProviderPhoneNumberWithParameter.GetEnumDescription());
                        _objectsSuccess.providerphoneNumberSuccess.Add(new ProviderPhoneNumberSuccess()
                        {
                            Ccx_id = data.SourceProviderPhoneNumberid,
                            Mc_id = dbvalue[0].ToString()
                        });
                        _logRepository.AddLogDetails(LogLevel.Info, SeverityLevels.Information.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"ProviderPhoneNumber object successfully stored into MedCompass DB for SourceProviderPhoneNumberid : {data.SourceProviderPhoneNumberid}");
                    }
                   
                    result = payLoadErrors.Count > 0 ? false : true;
                    payLoadErrors.Clear();
                }
            }
            catch (System.Exception ex)
            {
                _logRepository.AddLogDetails(LogLevel.Error, SeverityLevels.PartialCritical.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"System.Exception in Validation and Insertion of ProviderPhoneNumber object {ex.StackTrace}");
            }
            return result;
        }
        #endregion

        #region ValidateAndSaveProviderPostalAddress
        private async Task<bool> ValidateAndSaveProviderPostalAddress(List<Assure.ProviderPostalAddress> providerPostalAddress)
        {
            bool result = false;
            List<string> payLoadErrors = new List<string>();
            try
            {
                foreach (var data in providerPostalAddress)
                {
                    errorsModal = new List<ErrorsModal>();
                    var validationResult = _providerPostalAddressValidator.Validate(data);
                    if (validationResult.IsValid)
                    {
                        if (string.IsNullOrEmpty(_providerId))
                        {
                            var ProviderObj = await _getTableInfo.GetMedCompassDbData(data.SourceProviderId, ProviderInfoQueryEnum.QueryProviderWithParameter.GetEnumDescription());
                            _providerId = ProviderObj?["ProviderId"].ToString();
                        }
                        if (!string.IsNullOrEmpty(_providerId))
                        {
                            var providerPostalAddressObj = await _getTableInfo.GetMedCompassDbData(data.SourceProviderPostalAddressid, ProviderInfoQueryEnum.QueryProviderPostalAddressWithParameter.GetEnumDescription());
                            var mCProviderPostalAddressId = providerPostalAddressObj?["ProviderPostalAddressId"].ToString();
                            if (string.IsNullOrEmpty(mCProviderPostalAddressId) && _processId == _eventTypeIdValueAdd || !string.IsNullOrEmpty(mCProviderPostalAddressId) && _processId == _eventTypeIdValueNotify)
                            {
                                _providerRiaModel.providerPostalAddress.ProviderPostalAddressId = string.IsNullOrEmpty(mCProviderPostalAddressId) ? Guid.NewGuid() : new Guid(mCProviderPostalAddressId);
                                _providerRiaModel.providerPostalAddress.ProviderId = new Guid(_providerId);
                                _providerRiaModel.providerPostalAddress.Address1 = providerPostalAddressObj != null ? data.Address1.StringValue(providerPostalAddressObj["Address1"], _processId) : data.Address1.StringValue();
                                _providerRiaModel.providerPostalAddress.Address2 = providerPostalAddressObj != null ? data.Address2.StringValue(providerPostalAddressObj["Address2"], _processId) : data.Address2.StringValue();
                                _providerRiaModel.providerPostalAddress.Address3 = providerPostalAddressObj != null ? data.Address3.StringValue(providerPostalAddressObj["Address3"], _processId) : data.Address3.StringValue();
                                _providerRiaModel.providerPostalAddress.City = providerPostalAddressObj != null ? data.City.StringValue(providerPostalAddressObj["City"], _processId) : data.City.StringValue();
                                _providerRiaModel.providerPostalAddress.AddressStateId = providerPostalAddressObj != null ? data.AddressStateId.GuidValue(providerPostalAddressObj["AddressStateId"], _processId) : data.AddressStateId.GuidValue();
                                _providerRiaModel.providerPostalAddress.Zipcode = providerPostalAddressObj != null ? data.Zipcode.StringValue(providerPostalAddressObj["Zipcode"], _processId) : data.Zipcode.StringValue();
                                _providerRiaModel.providerPostalAddress.County = providerPostalAddressObj != null ? data.County.StringValue(providerPostalAddressObj["County"], _processId) : data.County.StringValue();
                                _providerRiaModel.providerPostalAddress.IslandTypeKey = providerPostalAddressObj != null ? data.IslandTypeKey.StringValue(providerPostalAddressObj["IslandTypeKey"], _processId) : data.IslandTypeKey.StringValue();
                                _providerRiaModel.providerPostalAddress.CountryTypeKey = providerPostalAddressObj != null ? data.CountryTypeKey.StringValue(providerPostalAddressObj["CountryTypeKey"], _processId) : data.CountryTypeKey.StringValue();
                                _providerRiaModel.providerPostalAddress.PostalAddressTypeKey = providerPostalAddressObj != null ? data.PostalAddressTypeKey.StringValue(providerPostalAddressObj["PostalAddressTypeKey"], _processId) : data.PostalAddressTypeKey.StringValue();
                                _providerRiaModel.providerPostalAddress.PrimaryAddress = (data.PrimaryAddress == "1") ? true : false;
                                _providerRiaModel.providerPostalAddress.Latitude = providerPostalAddressObj != null ? data.Latitude.DecimalValueNullable(providerPostalAddressObj["Latitude"], _processId) : data.Latitude.DecimalValueNullable();
                                _providerRiaModel.providerPostalAddress.Longitude = providerPostalAddressObj != null ? data.Longitude.DecimalValueNullable(providerPostalAddressObj["Longitude"], _processId) : data.Longitude.DecimalValueNullable();
                                _providerRiaModel.providerPostalAddress.ExpirationDate = providerPostalAddressObj != null ? data.ExpirationDate.DateTimeNullable(providerPostalAddressObj["ExpirationDate"], _processId) : data.ExpirationDate.DateTimeNullable();
                                _providerRiaModel.providerPostalAddress.EffectiveDate = providerPostalAddressObj != null ? data.EffectiveDate.DateTimeNotNull(providerPostalAddressObj["EffectiveDate"], _processId) : data.EffectiveDate.DateTimeNotNull();
                                _providerRiaModel.providerPostalAddress.ConversionId = data.SourceProviderPostalAddressid;
                                _providerRiaModel.providerPostalAddress.StateOrRegion = providerPostalAddressObj != null ? data.StateOrRegion.StringValue(providerPostalAddressObj["StateOrRegion"], _processId) : data.StateOrRegion.StringValue();
                                var changeSets = new List<ChangeSetEntry>();
                                if (string.IsNullOrEmpty(mCProviderPostalAddressId))
                                    changeSets.Add(new ChangeSetEntry { Id = 1, Entity = _providerRiaModel.providerPostalAddress, Operation = DomainOperation.Insert });
                                else
                                    changeSets.Add(new ChangeSetEntry { Id = 1, Entity = _providerRiaModel.providerPostalAddress, Operation = DomainOperation.Update });
                                var response = await _mCServices.RiaForProviderSubmitChanges(changeSets);
                                response[0].ValidationErrors.ForEach(x => { payLoadErrors.Add($"{x.ErrorCode}- {x.Message}"); });
                                response[0].ValidationErrors.ForEach(x =>
                                {
                                    errorsModal.Add(new ErrorsModal()
                                    {
                                        Error_code = x.ErrorCode.ToString(),
                                        Error_desc = x.Message
                                    });
                                });
                                changeSets.Clear();
                            }
                            else
                            {
                                if (_processId == _eventTypeIdValueAdd)
                                {
                                    payLoadErrors.Add($"{_eventTypeIdValueError} - ProviderPostalAddress not added (sourceproviderpostaladdressid:{data.SourceProviderPostalAddressid} previously added)");
                                    errorsModal.Add(new ErrorsModal()
                                    {
                                        Error_code = _eventTypeIdValueError,
                                        Error_desc = "ProviderPostalAddress not added(sourceproviderpostaladdressid:" + data.SourceProviderPostalAddressid + " previously added)"
                                    });
                                }
                                else
                                {
                                    errorsModal.Add(new ErrorsModal()
                                    {
                                        Error_code = _ParentProvider,
                                        Error_desc = "ProviderPostalAddress cannot be found (SourceProviderPostalAddressid: " + data.SourceProviderPostalAddressid + ")"
                                    });
                                    payLoadErrors.Add($"{_ParentProvider} - ProviderPostalAddress cannot be found(SourceProviderPostalAddressid:{data.SourceProviderPostalAddressid})");
                                }
                            }
                        }
                        else
                        {
                            errorsModal.Add(new ErrorsModal()
                            {
                                Error_code = _ParentProvider,
                                Error_desc = "Provider cannot be found(sourceproviderid " + data.SourceProviderId + ")"
                            });
                            payLoadErrors.Add($"{_ParentProvider} - Provider cannot be found(sourceproviderid:{data.SourceProviderId})");
                        }
                    }
                    else
                    {
                        foreach (var failure in validationResult.Errors)
                        {
                            errorsModal.Add(new ErrorsModal()
                            {
                                Error_code = failure.ErrorCode,
                                Error_desc = failure.PropertyName + " " + failure.ErrorMessage
                            });

                            payLoadErrors.Add($"{failure.ErrorCode} - {failure.PropertyName} {failure.ErrorMessage}");
                        }
                    }
                    if (payLoadErrors.Count > 0)
                    {
                        _logRepository.AddLogDetails(LogLevel.Info, SeverityLevels.Information.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"Ineligible ProviderPostalAddress payload object - {JsonConvert.SerializeObject(data)}");
                        _logRepository.AddLogDetails(LogLevel.Error, SeverityLevels.PartialCritical.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"ProviderPostalAddress object not a valid payload: {string.Join(",", payLoadErrors)}");
                        _objectsError.providerpostaladdressError.Add(new ProviderPostalAddressError()
                        {
                            Id = data.SourceProviderPostalAddressid,
                            Errors = errorsModal
                        });
                    }
                    else
                    {
                        var dbvalue = await _getTableInfo.GetMedCompassDbData(data.SourceProviderPostalAddressid, ProviderInfoQueryEnum.QueryProviderPostalAddressWithParameter.GetEnumDescription());
                        _objectsSuccess.providerpostaladdressSuccess.Add(new ProviderPostalAddressSuccess()
                        {
                            Ccx_id = data.SourceProviderPostalAddressid,
                            Mc_id = dbvalue[0].ToString()
                        });
                        _logRepository.AddLogDetails(LogLevel.Info, SeverityLevels.Information.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"ProviderPostalAddress object successfully stored into MedCompass DB for SourceProviderPostalAddressid : {data.SourceProviderPostalAddressid}");
                    }
                   
                    result = payLoadErrors.Count > 0 ? false : true;
                    payLoadErrors.Clear();
                }
            }
            catch (System.Exception ex)
            {
                _logRepository.AddLogDetails(LogLevel.Error, SeverityLevels.PartialCritical.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"System.Exception in Validation and Insertion of ProviderPostalAddress object {ex.StackTrace}");
            }
            return result;
        }
        #endregion

        #region ValidateAndSaveProviderIdentifier
        private async Task<bool> ValidateAndSaveProviderIdentifier(List<Assure.ProviderIdentifier> providerIdentifier)
        {
            bool result = false;
            List<string> payLoadErrors = new List<string>();
            try
            {
                foreach (var data in providerIdentifier)
                {
                    errorsModal = new List<ErrorsModal>();
                    var validationResult = _providerIdentifierValidator.Validate(data);
                    if (validationResult.IsValid)
                    {
                        if (string.IsNullOrEmpty(_providerId))
                        {
                            var providerObj = await _getTableInfo.GetMedCompassDbData(data.SourceProviderId, ProviderInfoQueryEnum.QueryProviderWithParameter.GetEnumDescription());
                            _providerId = providerObj?["ProviderId"].ToString();
                        }
                        if (!string.IsNullOrEmpty(_providerId))
                        {
                            var providerIdentifierObj = await _getTableInfo.GetMedCompassDbData(data.SourceProviderIdentifierId, ProviderInfoQueryEnum.QueryProviderIdentifierWithParameter.GetEnumDescription());
                            var mCProviderIdentifierId = providerIdentifierObj?["ProviderIdentifierId"].ToString();
                            if (string.IsNullOrEmpty(mCProviderIdentifierId) && _processId == _eventTypeIdValueAdd || !string.IsNullOrEmpty(mCProviderIdentifierId) && _processId == _eventTypeIdValueNotify)
                            {
                                _providerRiaModel.providerIdentifier.ProviderIdentifierId = string.IsNullOrEmpty(mCProviderIdentifierId) ? Guid.NewGuid() : new Guid(mCProviderIdentifierId);
                                _providerRiaModel.providerIdentifier.ProviderId = new Guid(_providerId);
                                _providerRiaModel.providerIdentifier.ProviderIdentifierTypeKey = providerIdentifierObj != null ? data.ProviderIdentifierTypeKey.StringValue(providerIdentifierObj["ProviderIdentifierTypeKey"], _processId) : data.ProviderIdentifierTypeKey.StringValue();
                                _providerRiaModel.providerIdentifier.ProviderOtherIdentifierTypeKey = providerIdentifierObj != null ? data.ProviderOtherIdentifierTypeKey.StringValue(providerIdentifierObj["ProviderOtherIdentifierTypeKey"], _processId) : data.ProviderOtherIdentifierTypeKey.StringValue();
                                _providerRiaModel.providerIdentifier.IdentifierValue = data.IdentifierValue;
                                _providerRiaModel.providerIdentifier.PrimaryIdentifier = (data.PrimaryIdentifier == "1") ? true : false;
                                _providerRiaModel.providerIdentifier.ExpirationDate = providerIdentifierObj != null ? data.ExpirationDate.DateTimeNullable(providerIdentifierObj["ExpirationDate"], _processId) : data.ExpirationDate.DateTimeNullable();
                                _providerRiaModel.providerIdentifier.EffectiveDate = providerIdentifierObj != null ? data.EffectiveDate.DateTimeNotNull(providerIdentifierObj["EffectiveDate"], _processId) : data.EffectiveDate.DateTimeNotNull();
                                _providerRiaModel.providerIdentifier.ConversionId = data.SourceProviderIdentifierId;

                                List<ChangeSetEntry> changeSets = new List<ChangeSetEntry>();
                                if (string.IsNullOrEmpty(mCProviderIdentifierId))
                                    changeSets.Add(new ChangeSetEntry { Id = 1, Entity = _providerRiaModel.providerIdentifier, Operation = DomainOperation.Insert });
                                else
                                    changeSets.Add(new ChangeSetEntry { Id = 1, Entity = _providerRiaModel.providerIdentifier, Operation = DomainOperation.Update });
                                var response = await _mCServices.RiaForProviderSubmitChanges(changeSets);
                                response[0].ValidationErrors.ForEach(x => { payLoadErrors.Add($"{x.ErrorCode}- {x.Message}"); });
                                response[0].ValidationErrors.ForEach(x =>
                                {
                                    errorsModal.Add(new ErrorsModal()
                                    {
                                        Error_code = x.ErrorCode.ToString(),
                                        Error_desc = x.Message
                                    });
                                });
                                changeSets.Clear();
                            }
                            else
                            {
                                if (_processId == _eventTypeIdValueAdd)
                                {
                                    payLoadErrors.Add($"{_eventTypeIdValueError} - ProviderIdentifier not added (sourceprovideridentifierid:{data.SourceProviderIdentifierId} previously added)");
                                    errorsModal.Add(new ErrorsModal()
                                    {
                                        Error_code = _eventTypeIdValueError,
                                        Error_desc = "ProviderIdentifier not added(sourceprovideridentifierid:" + data.SourceProviderIdentifierId + " previously added)"
                                    });
                                }
                                else
                                {
                                    errorsModal.Add(new ErrorsModal()
                                    {
                                        Error_code = _ParentProvider,
                                        Error_desc = "ProviderIdentifier cannot be found (SourceProviderIdentifierId: " + data.SourceProviderIdentifierId + ")"
                                    });
                                    payLoadErrors.Add($"{_ParentProvider} - ProviderIdentifier cannot be found(SourceProviderIdentifierId:{data.SourceProviderIdentifierId})");
                                }
                            }
                        }
                        else
                        {
                            errorsModal.Add(new ErrorsModal()
                            {
                                Error_code = _ParentProvider,
                                Error_desc = "Provider cannot be found(sourceproviderid " + data.SourceProviderId + ")"
                            });
                            payLoadErrors.Add($"{_ParentProvider} - Provider cannot be found(sourceproviderid:{data.SourceProviderId})");
                        }
                    }
                    else
                    {
                        foreach (var failure in validationResult.Errors)
                        {
                            errorsModal.Add(new ErrorsModal()
                            {
                                Error_code = failure.ErrorCode,
                                Error_desc = failure.PropertyName + " " + failure.ErrorMessage
                            });
                            payLoadErrors.Add($"{failure.ErrorCode} - {failure.PropertyName} {failure.ErrorMessage}");
                        }
                    }
                    if (payLoadErrors.Count > 0)
                    {
                        _logRepository.AddLogDetails(LogLevel.Info, SeverityLevels.Information.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"Ineligible ProviderIdentifier payload object - {JsonConvert.SerializeObject(data)}");
                        _logRepository.AddLogDetails(LogLevel.Error, SeverityLevels.PartialCritical.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"ProviderIdentifier object not a valid payload: {string.Join(",", payLoadErrors)}");
                        _objectsError.provideridentifierError.Add(new ProviderIdentifierError()
                        {
                            Id = data.SourceProviderIdentifierId,
                            Errors = errorsModal
                        });
                    }
                    else
                    {
                        var dbvalue = await _getTableInfo.GetMedCompassDbData(data.SourceProviderIdentifierId, ProviderInfoQueryEnum.QueryProviderIdentifierWithParameter.GetEnumDescription());
                        _objectsSuccess.provideridentifierSuccess.Add(new ProviderIdentifierSuccess()
                        {
                            Ccx_id = data.SourceProviderIdentifierId,
                            Mc_id = dbvalue[0].ToString()
                        });
                        _logRepository.AddLogDetails(LogLevel.Info, SeverityLevels.Information.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"ProviderIdentifier object successfully stored into MedCompass DB for SourceProviderIdentifierId : {data.SourceProviderIdentifierId}");
                    }
                   
                    result = payLoadErrors.Count > 0 ? false : true;
                    payLoadErrors.Clear();
                }
            }
            catch (System.Exception ex)
            {
                _logRepository.AddLogDetails(LogLevel.Error, SeverityLevels.PartialCritical.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"System.Exception in Validation and Insertion of ProviderIdentifier object {ex.StackTrace}");
            }
            return result;
        }
        #endregion

        #region ValidateAndSaveProviderNetwork
        private async Task<bool> ValidateAndSaveProviderNetwork(List<Assure.ProviderNetwork> providerNetwork)
        {
            bool result = false;
            List<string> payLoadErrors = new List<string>();
            try
            {
                foreach (var data in providerNetwork)
                {
                    errorsModal = new List<ErrorsModal>();
                    var validationResult = _providerNetworkValidator.Validate(data);
                    if (validationResult.IsValid)
                    {
                        if (string.IsNullOrEmpty(_providerId))
                        {
                            var providerObj = await _getTableInfo.GetMedCompassDbData(data.SourceProviderId, ProviderInfoQueryEnum.QueryProviderWithParameter.GetEnumDescription());
                            _providerId = providerObj?["ProviderId"].ToString();
                        }
                        if (!string.IsNullOrEmpty(_providerId))
                        {
                            var providerNetworkObj = await _getTableInfo.GetMedCompassDbData(data.SourceProviderNetworkId, ProviderInfoQueryEnum.QueryProviderNetworkWithParameter.GetEnumDescription());
                            var mCProviderNetworkId = providerNetworkObj?["ProviderNetworkId"].ToString();
                            if (string.IsNullOrEmpty(mCProviderNetworkId) && _processId == _eventTypeIdValueAdd || !string.IsNullOrEmpty(mCProviderNetworkId) && _processId == _eventTypeIdValueNotify)
                            {
                                _providerRiaModel.providerNetwork.ProviderNetworkId = string.IsNullOrEmpty(mCProviderNetworkId) ? Guid.NewGuid() : new Guid(mCProviderNetworkId);
                                _providerRiaModel.providerNetwork.Identifier = providerNetworkObj != null ? data.Identifier.StringValue(providerNetworkObj["Identifier"], _processId) : data.Identifier.StringValue();
                                _providerRiaModel.providerNetwork.Name = providerNetworkObj != null ? data.Name.StringValue(providerNetworkObj["Name"], _processId) : data.Name.StringValue();
                                _providerRiaModel.providerNetwork.ParNonPar = providerNetworkObj != null ? data.ParNonPar.StringValue(providerNetworkObj["ParNonPar"], _processId) : data.ParNonPar.StringValue();
                                _providerRiaModel.providerNetwork.BenefitTier = providerNetworkObj != null ? data.BenefitTier.StringValue(providerNetworkObj["BenefitTier"], _processId) : data.BenefitTier.StringValue();
                                _providerRiaModel.providerNetwork.ProviderNetworkTypeKey = providerNetworkObj != null ? data.ProviderNetworkTypeKey.StringValue(providerNetworkObj["ProviderNetworkTypeKey"], _processId) : data.ProviderNetworkTypeKey.StringValue();
                                _providerRiaModel.providerNetwork.ExpirationDate = providerNetworkObj != null ? data.ExpirationDate.DateTimeNullable(providerNetworkObj["ExpirationDate"], _processId) : data.ExpirationDate.DateTimeNullable();
                                _providerRiaModel.providerNetwork.EffectiveDate = providerNetworkObj != null ? data.EffectiveDate.DateTimeNotNull(providerNetworkObj["EffectiveDate"], _processId) : data.EffectiveDate.DateTimeNotNull();
                                _providerRiaModel.providerNetwork.ConversionId = data.SourceProviderNetworkId;
                                var changeSets = new List<ChangeSetEntry>();
                                if (string.IsNullOrEmpty(mCProviderNetworkId))
                                    changeSets.Add(new ChangeSetEntry { Id = 1, Entity = _providerRiaModel.providerNetwork, Operation = DomainOperation.Insert });
                                else
                                    changeSets.Add(new ChangeSetEntry { Id = 1, Entity = _providerRiaModel.providerNetwork, Operation = DomainOperation.Update });
                                var response = await _mCServices.RiaForProviderSubmitChanges(changeSets);
                                response[0].ValidationErrors.ForEach(x => { payLoadErrors.Add($"{x.ErrorCode}- {x.Message}"); });
                                response[0].ValidationErrors.ForEach(x =>
                                {
                                    errorsModal.Add(new ErrorsModal()
                                    {
                                        Error_code = x.ErrorCode.ToString(),
                                        Error_desc = x.Message
                                    });
                                });
                                changeSets.Clear();
                            }
                            else
                            {
                                if (_processId == _eventTypeIdValueAdd)
                                {
                                    payLoadErrors.Add($"{_eventTypeIdValueError} - ProviderNetwork not added (sourceprovidernetworkid:{data.SourceProviderNetworkId} previously added)");
                                    errorsModal.Add(new ErrorsModal()
                                    {
                                        Error_code = _eventTypeIdValueError,
                                        Error_desc = "ProviderNetwork not added(sourceprovidernetworkid:" + data.SourceProviderNetworkId + " previously added)"
                                    });
                                }
                                else
                                {
                                    errorsModal.Add(new ErrorsModal()
                                    {
                                        Error_code = _ParentProvider,
                                        Error_desc = "ProviderNetwork cannot be found (sourceprovidernetworkid: " + data.SourceProviderNetworkId + ")"
                                    });
                                    payLoadErrors.Add($"{_ParentProvider} - ProviderNetwork cannot be found (sourceprovidernetworkid:{data.SourceProviderNetworkId})");
                                }
                            }
                        }
                        else
                        {
                            errorsModal.Add(new ErrorsModal()
                            {
                                Error_code = _ParentProvider,
                                Error_desc = "Provider cannot be found (sourceproviderid: " + data.SourceProviderId + ")"
                            });
                            payLoadErrors.Add($"{_ParentProvider} - Provider cannot be found(sourceproviderid:{data.SourceProviderId})");
                        }
                    }
                    else
                    {
                        foreach (var failure in validationResult.Errors)
                        {
                            errorsModal.Add(new ErrorsModal()
                            {
                                Error_code = failure.ErrorCode,
                                Error_desc = failure.PropertyName + " " + failure.ErrorMessage
                            });
                            payLoadErrors.Add($"{failure.ErrorCode} - {failure.PropertyName} {failure.ErrorMessage}");
                        }
                    }
                    if (payLoadErrors.Count > 0)
                    {
                        _logRepository.AddLogDetails(LogLevel.Info, SeverityLevels.Information.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"Ineligible ProviderNetwork payload object - {JsonConvert.SerializeObject(data)}");
                        _logRepository.AddLogDetails(LogLevel.Error, SeverityLevels.PartialCritical.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"ProviderNetwork object not a valid payload: {string.Join(",", payLoadErrors)}");
                        _objectsError.providernetworkError.Add(new ProviderNetworkError()
                        {
                            Id = data.SourceProviderNetworkId,
                            Errors = errorsModal
                        });
                    }
                    else
                    {
                        var dbvalue = await _getTableInfo.GetMedCompassDbData(data.SourceProviderNetworkId, ProviderInfoQueryEnum.QueryProviderNetworkWithParameter.GetEnumDescription());
                        _objectsSuccess.providernetworkSuccess.Add(new ProviderNetworkSuccess()
                        {
                            Ccx_id = data.SourceProviderNetworkId,
                            Mc_id = dbvalue[0].ToString()
                        });
                        _logRepository.AddLogDetails(LogLevel.Info, SeverityLevels.Information.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"ProviderNetwork object successfully stored into MedCompass DB for SourceProviderNetworkId:{data.SourceProviderNetworkId}");
                    }
                    
                    result = payLoadErrors.Count == 0 ? false : true;
                    payLoadErrors.Clear();
                }
            }
            catch (System.Exception ex)
            {
                _logRepository.AddLogDetails(LogLevel.Error, SeverityLevels.PartialCritical.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"System.Exception in Validation and Insertion of ProviderNetwork object {ex.StackTrace}");
            }
            return result;
        }
        #endregion

        #region IsValidateProviderSourceId
        private bool IsValidateProviderSourceId(ProviderDetails providerdata)
        {
            string cCxSourceProviderId = string.Empty;
            try
            {
                if (providerdata.Provider != null)
                {
                    cCxSourceProviderId = providerdata.Provider.SourceProviderId;
                    if (providerdata.Provider.ProviderPostalAddress != null && providerdata.Provider.ProviderPostalAddress.Count > 0)
                    {
                        foreach (var data in providerdata.Provider.ProviderPostalAddress)
                        {
                            if (string.IsNullOrEmpty(data.SourceProviderId) || data.SourceProviderId != cCxSourceProviderId)
                                return false;
                        }
                    }
                    if (providerdata.Provider.ProviderContract != null && providerdata.Provider.ProviderContract.Count > 0)
                    {
                        foreach (var data in providerdata.Provider.ProviderContract)
                        {
                            if (string.IsNullOrEmpty(data.SourceProviderId) || data.SourceProviderId != cCxSourceProviderId)
                                return false;
                        }
                    }
                    if (providerdata.Provider.ProviderEmailAddress != null && providerdata.Provider.ProviderEmailAddress.Count > 0)
                    {
                        foreach (var data in providerdata.Provider.ProviderEmailAddress)
                        {
                            if (string.IsNullOrEmpty(data.SourceProviderId) || data.SourceProviderId != cCxSourceProviderId)
                                return false;
                        }
                    }
                    if (providerdata.Provider.ProviderIdentifier != null && providerdata.Provider.ProviderIdentifier.Count > 0)
                    {
                        foreach (var data in providerdata.Provider.ProviderIdentifier)
                        {
                            if (string.IsNullOrEmpty(data.SourceProviderId) || data.SourceProviderId != cCxSourceProviderId)
                                return false;
                        }
                    }
                    if (providerdata.Provider.ProviderPhoneNumber != null && providerdata.Provider.ProviderPhoneNumber.Count > 0)
                    {
                        foreach (var data in providerdata.Provider.ProviderPhoneNumber)
                        {
                            if (string.IsNullOrEmpty(data.SourceProviderId) || data.SourceProviderId != cCxSourceProviderId)
                                return false;
                        }
                    }
                    if (providerdata.Provider.MemberProvider != null && providerdata.Provider.MemberProvider.Count > 0)
                    {
                        foreach (var data in providerdata.Provider.MemberProvider)
                        {
                            if (string.IsNullOrEmpty(data.SourceProviderId) || data.SourceProviderId != cCxSourceProviderId)
                                return false;
                        }
                    }
                    if (providerdata.Provider.ProviderNetwork != null && providerdata.Provider.ProviderNetwork.Count > 0)
                    {
                        foreach (var data in providerdata.Provider.ProviderNetwork)
                        {
                            if (string.IsNullOrEmpty(data.SourceProviderId) || data.SourceProviderId != cCxSourceProviderId)
                                return false;
                        }
                    }
                }
                if (providerdata.ProviderPostalAddress != null && providerdata.ProviderPostalAddress.Count > 0)
                {
                    foreach (var data in providerdata.ProviderPostalAddress)
                    {
                        if (string.IsNullOrEmpty(cCxSourceProviderId))
                            cCxSourceProviderId = data.SourceProviderId;
                        if (string.IsNullOrEmpty(data.SourceProviderId) || data.SourceProviderId != cCxSourceProviderId)
                            return false;
                    }
                }
                if (providerdata.ProviderContract != null && providerdata.ProviderContract.Count > 0)
                {
                    foreach (var data in providerdata.ProviderContract)
                    {
                        if (string.IsNullOrEmpty(cCxSourceProviderId))
                            cCxSourceProviderId = data.SourceProviderId;
                        if (string.IsNullOrEmpty(data.SourceProviderId) || data.SourceProviderId != cCxSourceProviderId)
                            return false;
                    }
                }
                if (providerdata.ProviderEmailAddress != null && providerdata.ProviderEmailAddress.Count > 0)
                {
                    foreach (var data in providerdata.ProviderEmailAddress)
                    {
                        if (string.IsNullOrEmpty(cCxSourceProviderId))
                            cCxSourceProviderId = data.SourceProviderId;
                        if (string.IsNullOrEmpty(data.SourceProviderId) || data.SourceProviderId != cCxSourceProviderId)
                            return false;
                    }
                }
                if (providerdata.ProviderIdentifier != null && providerdata.ProviderIdentifier.Count > 0)
                {
                    foreach (var data in providerdata.ProviderIdentifier)
                    {
                        if (string.IsNullOrEmpty(cCxSourceProviderId))
                            cCxSourceProviderId = data.SourceProviderId;
                        if (string.IsNullOrEmpty(data.SourceProviderId) || data.SourceProviderId != cCxSourceProviderId)
                            return false;
                    }
                }
                if (providerdata.ProviderPhoneNumber != null && providerdata.ProviderPhoneNumber.Count > 0)
                {
                    foreach (var data in providerdata.ProviderPhoneNumber)
                    {
                        if (string.IsNullOrEmpty(cCxSourceProviderId))
                            cCxSourceProviderId = data.SourceProviderId;
                        if (string.IsNullOrEmpty(data.SourceProviderId) || data.SourceProviderId != cCxSourceProviderId)
                            return false;
                    }
                }
                if (providerdata.MemberProvider != null && providerdata.MemberProvider.Count > 0)
                {
                    foreach (var data in providerdata.MemberProvider)
                    {
                        if (string.IsNullOrEmpty(cCxSourceProviderId))
                            cCxSourceProviderId = data.SourceProviderId;
                        if (string.IsNullOrEmpty(data.SourceProviderId) || data.SourceProviderId != cCxSourceProviderId)
                            return false;
                    }
                }
                if (providerdata.ProviderNetwork != null && providerdata.ProviderNetwork.Count > 0)
                {
                    foreach (var data in providerdata.ProviderNetwork)
                    {
                        if (string.IsNullOrEmpty(cCxSourceProviderId))
                            cCxSourceProviderId = data.SourceProviderId;
                        if (string.IsNullOrEmpty(data.SourceProviderId) || data.SourceProviderId != cCxSourceProviderId)
                            return false;
                    }
                }
                return true;
            }
            catch (System.Exception ex)
            {
                _logRepository.AddLogDetails(LogLevel.Error, SeverityLevels.PartialCritical.GetEnumDescription(), _titleForLog, MethodBase.GetCurrentMethod().Name, _processId, this.GetType().Name, _currentThreadId, $"System.Exception in Validation of Provider payload {ex.StackTrace}");
                return false;
            }
        }

        #endregion
        private bool IsValidateFullorDiscreteJsonPayload(ProviderDetails providerdata)
        {
            if (providerdata.Provider != null && (providerdata.ProviderContract != null || providerdata.ProviderEmailAddress != null
                || providerdata.ProviderIdentifier != null || providerdata.ProviderNetwork != null || providerdata.ProviderPhoneNumber != null
                || providerdata.ProviderPostalAddress != null || providerdata.MemberProvider != null))
                return false;
            return true;
        }

        private async Task<bool> JsonResultPost(Uri url, string value)
        {
            

            return true;
        }
        

    }
}
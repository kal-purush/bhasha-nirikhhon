using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Lykke.Service.ClientAccount.Client.Models;
using Lykke.Service.Kyc.Abstractions.Domain.Verification;
using Lykke.Service.Kyc.Abstractions.Services;
using Lykke.Service.Tier.Domain;
using Lykke.Service.Tier.Domain.Services;

namespace Lykke.Service.Tier.DomainServices
{
    public class TiersService : ITiersService
    {
        private readonly ILimitsService _limitsService;
        private readonly ISettingsService _settingsService;
        private readonly ITierUpgradeService _tierUpgradeService;
        private readonly IKycStatusService _kycStatusService;
        private readonly IKycDocumentsService _kycDocumentsService;

        public TiersService(
            ILimitsService limitsService,
            ISettingsService settingsService,
            ITierUpgradeService tierUpgradeService,
            IKycStatusService kycStatusService,
            IKycDocumentsService kycDocumentsService

            )
        {
            _limitsService = limitsService;
            _settingsService = settingsService;
            _tierUpgradeService = tierUpgradeService;
            _kycStatusService = kycStatusService;
            _kycDocumentsService = kycDocumentsService;
        }

        public async Task<ClientTierInfo> GetClientTierInfoAsync(string clientId, AccountTier clientTier, string country)
        {
            CurrentTier currentTier = await GetCurrentTierAync(clientId, clientTier, country);
            TierUpgradeRequest upgradeRequet = await GetUpgradeRequestAsync(clientId, clientTier, country);
            NextTier nextTier = await GetNextTierAsync(clientId, clientTier, country, upgradeRequet);

            var result = new ClientTierInfo
            {
                CurrentTier = currentTier,
                NextTier = nextTier,
                UpgradeRequest = upgradeRequet
            };

            return result;
        }

        private async Task<CurrentTier> GetCurrentTierAync(string clientId, AccountTier clientTier, string country)
        {
            var currentDepositAmountTask = _limitsService.GetClientDepositAmountAsync(clientId, clientTier);
            var maxLimitTask = _limitsService.GetClientLimitSettingsAsync(clientId, clientTier, country);

            await Task.WhenAll(currentDepositAmountTask, maxLimitTask);

            return new CurrentTier
            {
                Tier = clientTier,
                Asset = _settingsService.GetDefaultAsset(),
                Current = currentDepositAmountTask.Result,
                MaxLimit = maxLimitTask.Result?.MaxLimit ?? 0
            };
        }

        internal async Task<TierUpgradeRequest> GetUpgradeRequestAsync(string clientId, AccountTier clientTier, string country)
        {
            var tierUpgradeRequestsTask = _tierUpgradeService.GetByClientAsync(clientId);
            var kycStatusTask = _kycStatusService.GetKycStatusAsync(clientId);

            await Task.WhenAll(tierUpgradeRequestsTask, kycStatusTask);

            IReadOnlyList<ITierUpgradeRequest> tierUpgradeRequests = tierUpgradeRequestsTask.Result;
            KycStatus kycStatus = kycStatusTask.Result;
            bool isHighRiskCountry = _settingsService.IsHighRiskCountry(country);

            if (clientTier == AccountTier.Beginner && !isHighRiskCountry && kycStatus != KycStatus.Ok &&
                kycStatus != KycStatus.NeedToFillData && !tierUpgradeRequests.Any())
            {
                var docs = (await _kycDocumentsService.GetDocumentsAsync(clientId)).ToList();
                var date = docs.OrderByDescending(x => x.DateTime).FirstOrDefault()?.DateTime;

                var kycStatuses = new List<KycStatus>
                {
                    KycStatus.Pending, KycStatus.ReviewDone, KycStatus.JumioInProgress, KycStatus.JumioOk
                };

                if (docs.Any() && date.HasValue)
                {
                    return new TierUpgradeRequest
                    {
                        Tier = AccountTier.Apprentice,
                        Status = kycStatuses.Contains(kycStatus)
                            ? KycStatus.Pending.ToString()
                            : KycStatus.Rejected.ToString(),
                        SubmitDate = date.Value
                    };
                }
            }

            var rejectedRequest = tierUpgradeRequests
                .OrderBy(x => x.Tier)
                .FirstOrDefault(x => x.KycStatus != KycStatus.Ok && x.KycStatus != KycStatus.Pending);

            if (rejectedRequest != null)
            {
                return new TierUpgradeRequest
                {
                    Tier = rejectedRequest.Tier,
                    Status = rejectedRequest.KycStatus.ToString(),
                    SubmitDate = rejectedRequest.Date
                };
            }

            var lastPendingRequest = tierUpgradeRequests
                .Where(x => x.KycStatus != KycStatus.Ok)
                .OrderByDescending(x => x.Tier)
                .FirstOrDefault();

            if (lastPendingRequest != null)
            {
                return new TierUpgradeRequest
                {
                    Tier = lastPendingRequest.Tier,
                    Status = lastPendingRequest.KycStatus.ToString(),
                    SubmitDate = lastPendingRequest.Date
                };
            }

            return null;
        }

        internal async Task<NextTier> GetNextTierAsync(string clientId, AccountTier clientTier, string country, TierUpgradeRequest upgradeRequest)
        {
            AccountTier? nextTier = GetNextTier(clientTier, country, upgradeRequest);

            if (nextTier.HasValue)
            {
                var nextTierLimits = await _limitsService.GetClientLimitSettingsAsync(clientId, nextTier.Value, country);

                if (nextTierLimits != null)
                {
                    return new NextTier
                    {
                        Tier = nextTier.Value,
                        MaxLimit = nextTierLimits.MaxLimit ?? 0,
                        Documents = nextTierLimits.Documents.Select(x => x.ToString()).ToArray(),
                    };
                }
            }

            return null;
        }

        internal AccountTier? GetNextTier(AccountTier clientTier, string country, TierUpgradeRequest upgradeRequest)
        {
            AccountTier tier = clientTier;

            if (upgradeRequest != null)
            {
                switch (upgradeRequest.Status)
                {
                    case nameof(KycStatus.Rejected):
                        return upgradeRequest.Tier;
                    case nameof(KycStatus.Pending):
                        tier = upgradeRequest.Tier;
                        break;
                }
            }

            if (tier == AccountTier.ProIndividual)
                return null;

            bool isHighRiskCountry = _settingsService.IsHighRiskCountry(country);

            if (isHighRiskCountry)
                return AccountTier.ProIndividual;

            var values = (AccountTier[]) Enum.GetValues(typeof(AccountTier));

            return values[(int) tier + 1];
        }
    }
}
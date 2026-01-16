using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using NewVerificationProvider.Interfaces;
using NewVerificationProvider.Services;

namespace NewVerificationProvider.Functions;

public class VerificationCleaner(ILogger<VerificationCleanerSerivce> logger, IVerificationCleanerSerivce verificationCleanerSerivce)
{
    private readonly ILogger<VerificationCleanerSerivce> _logger = logger;
    private readonly IVerificationCleanerSerivce _verificationCleanerSerivce = verificationCleanerSerivce;


    [Function("VerificationCleaner")]
    public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo myTimer)
    {
        try
        {
            await _verificationCleanerSerivce.RemoveExpiredRecordsAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError($"ERROR : VerificationCleaner.Run() :: {ex.Message} ");
        }
    }
}
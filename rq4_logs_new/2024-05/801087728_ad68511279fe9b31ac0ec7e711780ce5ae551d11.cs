using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using NewVerificationProvider.Interfaces;

namespace NewVerificationProvider.Functions;

public class GenerateVerificationCode(ILogger<GenerateVerificationCode> logger, IVerificationService verificationService)
{
    private readonly ILogger<GenerateVerificationCode> _logger = logger;
    private readonly IVerificationService _verificationService = verificationService;


    [Function(nameof(GenerateVerificationCode))]
    [ServiceBusOutput("email_request", Connection = "ServiceBusConnection")]
    public async Task<string> Run([ServiceBusTrigger("verification_request", Connection = "ServiceBusConnection")] ServiceBusReceivedMessage message, ServiceBusMessageActions messageActions)
    {
        try
        {
            var verificationRequst = _verificationService.UnPackVerificationRequest(message);
            if (verificationRequst != null)
            {
                var code = _verificationService.GenerateCode();
                if (!string.IsNullOrEmpty(code))
                {
                    var result = await _verificationService.SaveVerificationRequest(verificationRequst, code);
                    if (result)
                    {
                        var emailRequest = _verificationService.GenerateEmailRequest(verificationRequst, code);
                        if (emailRequest != null)
                        {
                            var payload = _verificationService.GenerateServiceBusEmailRequest(emailRequest);
                            if (!string.IsNullOrEmpty(payload))
                            {
                                await messageActions.CompleteMessageAsync(message);
                                return payload;
                            }
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError($"ERROR : GenerateVerificationCode.Run() :: {ex.Message} ");
        }
        return null!;
    }
}
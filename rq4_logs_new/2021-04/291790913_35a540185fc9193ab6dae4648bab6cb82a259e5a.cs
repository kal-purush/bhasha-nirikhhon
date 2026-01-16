using LT.DigitalOffice.Broker.Requests;
using LT.DigitalOffice.Kernel.Broker;
using MassTransit;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace LT.DigitalOffice.UserService.Business.Helpers.Email
{
    public class EmailResender
    {
        private static IRequestClient<ISendEmailRequest> _rcSendEmail;
        private static ILogger<EmailResender> _logger;

        private static readonly List<object> _emailRequests = new();

        public EmailResender(
            IRequestClient<ISendEmailRequest> rcSendEmail,
            ILogger<EmailResender> logger)
        {
            _rcSendEmail = rcSendEmail;
            _logger = logger;
        }

        public static void AddToQueue(object emailRequest)
        {
            _emailRequests.Add(emailRequest);
        }

        public static void Start(double intervalInMinutes)
        {
            while (true)
            {
                Task.Delay(TimeSpan.FromMinutes(intervalInMinutes)).Wait();

                for (int i = 0; i < _emailRequests.Count; i++)
                {
                    try
                    {
                        IOperationResult<bool> rcSendEmailResponse = 
                            _rcSendEmail.GetResponse<IOperationResult<bool>>(_emailRequests[i]).Result.Message;
                        if (rcSendEmailResponse.IsSuccess)
                        {
                            _emailRequests.RemoveAt(i);
                        }
                        else
                        {
                            // TODO: log mailing address
                            _logger.LogWarning(
                                $"Can not send email. Email remain in resend queue:" +
                                $"{Environment.NewLine}{string.Join('\n', rcSendEmailResponse.Errors)}");
                        }
                    }
                    catch (Exception exc)
                    {
                        // TODO: log mailing address
                        _logger.LogError(exc, $"Errors while sending email. Email remain in resend queue.");
                    }
                }
            }
        }
    }
}
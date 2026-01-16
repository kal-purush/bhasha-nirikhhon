using EmailService.Application.Repositories.SentMail;
using EmailService.Domain;
using Mambo.MassTransit.Contracts.Events.Abstracts;
using Mambo.MassTransit.Contracts.Events.Commands.Abstracts;
using MassTransit;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EmailService.Application.Consumers
{
    public sealed class SendEmailToUserConsumer(ILogger<SendEmailToUserConsumer> _logger, ISentMailWriteRepository _sentMailWriteRepository) : IConsumer<ISendEmailToUserEvent>
    {
        private const string _mailReason = "SignIn";

        public async Task Consume(ConsumeContext<ISendEmailToUserEvent> context)
        {
            var messageFromQueue = context.Message;
            try
            {
                await _sentMailWriteRepository.CreateSingleDocumentAsync(SentMailEntity.CreateSentMail(messageFromQueue.UserId, messageFromQueue.Email, _mailReason), context.CancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError("An unexpected error occurred while sending email or saving mail record to database. Message: {@Message}. Error: {@ErrorMessage}", messageFromQueue.ToString(), ex);
            }
        }
    }
}
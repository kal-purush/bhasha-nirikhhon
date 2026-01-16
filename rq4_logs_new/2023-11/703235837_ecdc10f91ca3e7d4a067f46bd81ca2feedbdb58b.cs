using Muscler.Domain.Entity.Auth;
using SendGrid;
using SendGrid.Helpers.Mail;

namespace Muscler.Domain.Email
{
    public class EmailService : IEmailService
    {
        public ISendGridClient Client { get; }

        private const string NoReplyEmail = "no-reply@muscler.pro";
        private const string NoReplyName = "Muscler";
        private EmailAddress FromEmailAddress { get; } = new(NoReplyEmail, NoReplyName);

        public EmailService(ISendGridClient client)
        {
            Client = client;
        }

        public async Task<Response?> SendConfirmationLink(string confirmationLink, ApplicationUser user)
        {
            var subject = "Confirm your account";

            var to = new EmailAddress(user.Email, user.UserName);

            var plainTextContent = $"Click on this link to confirm your Muscler.pro account : {confirmationLink}";

            var htmlContent = $"<strong>Click on this link to confirm your Muscler.pro account : {confirmationLink}</strong>";

            var msg = MailHelper.CreateSingleEmail(FromEmailAddress, to, subject, plainTextContent, htmlContent);

            return await Client.SendEmailAsync(msg);
        }
    }
}
//-----------------------------------------------------------------------
// <copyright file="GtdTimerEmailSender.cs" company="SoftServe">
//     Company copyright tag.
// </copyright>
//-----------------------------------------------------------------------

using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

using GtdCommon.IoC;

namespace GtdCommon.Email
{
    /// <summary>
    /// Handles sending emails specific to the GtdTimer project
    /// </summary>
    public class GtdTimerEmailSender
    {
        /// <summary>
        /// Sends a verification email to the specified user
        /// </summary>
        /// <param name="displayName">The users display name (typically first name)</param>
        /// <param name="email">The users email to be verified</param>
        /// <param name="verificationUrl">The URL the user needs to click to verify their email</param>
        /// <returns>return result of sending email</returns>
        public static async Task<SendEmailResponse> SendUserVerificationEmailAsync(string displayName, string email, string verificationUrl)
        {
            return await IoCContainer.EmailTemplateSender.SendGeneralEmailAsync(new SendEmailDetails
            {
                Content = IoCContainer.Configuration.GetValue<string>("GtdTimerEmailSettings:Content"),
                IsHTML = true,
                FromEmail = IoCContainer.Configuration.GetValue<string>("GtdTimerEmailSettings:SendEmailFromEmail"),
                FromName = IoCContainer.Configuration.GetValue<string>("GtdTimerEmailSettings:SendEmailFromEmail"),
                ToEmail = email,
                ToName = displayName,
                Subject = IoCContainer.Configuration.GetValue<string>("GtdTimerEmailSettings:Subject")
            }, verificationUrl);
        }
    }
}
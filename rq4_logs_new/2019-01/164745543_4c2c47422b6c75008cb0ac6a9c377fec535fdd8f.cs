using System;
using System.Collections.Generic;
using System.Linq;
using Twilio;
using Twilio.Rest.Api.V2010.Account;
using System.Threading.Tasks;

namespace cruzhacks_2019_announcments_service.Models
{
    public class TwilioAnnouncmentClient
    {
        private List<string> _phoneNumbers = new List<string>();
        private string _accountNumber;
        private string _accountSid;
        private string _authToken;

        public TwilioAnnouncmentClient()
        {
            _accountSid = Environment.GetEnvironmentVariable("TWILIO_ID");
            _authToken = Environment.GetEnvironmentVariable("TWILIO_TOKEN");
            _accountNumber = Environment.GetEnvironmentVariable("TWILIO_ACCOUNT_PHONE_NUMBER");
            TwilioClient.Init(_accountSid, _authToken);
            _phoneNumbers.Add("7072921668");
        }

        public void SendAnnouncment(string messageTitle, string messageBody)
        {
            var twilioAccountNumber = new Twilio.Types.PhoneNumber(_accountNumber);

            foreach (string number in _phoneNumbers)
            {
                var message = MessageResource.Create(
                    body: messageBody,
                    from: twilioAccountNumber,
                    to: new Twilio.Types.PhoneNumber("+1" + number)
                );
                Console.WriteLine(message.Sid);
            }
        }
    }
}
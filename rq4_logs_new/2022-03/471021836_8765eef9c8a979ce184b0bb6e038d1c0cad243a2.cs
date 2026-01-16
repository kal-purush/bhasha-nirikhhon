using Microsoft.Graph;
using System;
using System.Activities;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Impower.Office365.Mail
{
    public abstract class MessageActivity : Office365Activity
    {
        [Category("Input")]
        [RequiredArgument]
        [Description("MessageID of the target message")]
        public InArgument<string> MessageID { get; set; }
        [Category("Input")]
        [Description("Email address of user the email is associated with")]
        [RequiredArgument]
        public InArgument<string> Email { get; set; }
        protected string EmailValue { get; set; }
        protected string MessageIdValue { get; set; }

        protected override void ReadContext(AsyncCodeActivityContext context)
        {
            EmailValue = context.GetValue(Email);
            MessageIdValue = context.GetValue(MessageID);
        }
        protected override Task Initialize(GraphServiceClient client, AsyncCodeActivityContext context, CancellationToken token) { return Task.CompletedTask; }
    }
}
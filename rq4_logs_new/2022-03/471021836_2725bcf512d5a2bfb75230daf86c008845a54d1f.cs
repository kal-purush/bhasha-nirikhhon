using Microsoft.Graph;
using System;
using System.Activities;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Impower.Office365.Mail
{
    [DisplayName("Download Message as .EML")]
    public class GetMessageAsEML : Office365Activity
    {
        [Category("Input")]
        [RequiredArgument]
        [Description("MessageID of the target message")]
        public InArgument<string> MessageID { get; set; }
        [Category("Input")]
        [Description("Email address of user the email is associated with")]
        [RequiredArgument]
        public InArgument<string> Email { get; set; }
        [Category("Input")]
        [Description("Where To Save The Email")]
        [DefaultValue(true)]
        public InArgument<string> FilePath { get; set; }
        private string MessageIdValue;
        private string EmailValue;
        private string FilePathValue;

        protected override async Task<Action<AsyncCodeActivityContext>> ExecuteAsyncWithClient(CancellationToken token, GraphServiceClient client)
        {
            var stream = await client.GetMessageAsEML(token, MessageIdValue, EmailValue);
            using (FileStream outputFileStream = new FileStream(FilePathValue, FileMode.Create))
            {
                stream.CopyTo(outputFileStream);
            }
            return ctx => { Expression.Empty(); };
        }

        protected override Task Initialize(GraphServiceClient client, AsyncCodeActivityContext context, CancellationToken token)
        {
            return Task.CompletedTask;
        }
        protected override void ReadContext(AsyncCodeActivityContext context)
        {
            MessageIdValue = MessageID.Get(context);
            EmailValue = Email.Get(context);
            FilePathValue = FilePath.Get(context);
        }
    }
}
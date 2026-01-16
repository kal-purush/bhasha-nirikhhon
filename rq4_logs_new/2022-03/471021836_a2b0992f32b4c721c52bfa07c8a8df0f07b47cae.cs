using Microsoft.Graph;
using System;
using System.Activities;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Impower.Office365.Sharepoint
{
    public class GetDriveItemFromSharingLink : Office365Activity
    {
        [RequiredArgument]
        [Category("Input")]
        public InArgument<string> SharingURL { get; set; }
        internal string sharingUrl;

        [Category("Output")]
        public OutArgument<DriveItem> DriveItem { get; set; }
        [Category("Output")]
        public OutArgument<ListItem> ListItem { get; set; }
        [Category("Output")]
        public OutArgument<Site> Site { get; set; }

        //[Category("Output")]
        //public OutArgument<SharedDriveItem> SharedDriveItem { get; set; }
        //[Category("Output")]
        //public OutArgument<Site> Site { get; set; }
        protected override Task Initialize(GraphServiceClient client, AsyncCodeActivityContext context, CancellationToken token)
        {
            return Task.CompletedTask;
        }
        protected override void ReadContext(AsyncCodeActivityContext context)
        {
            this.sharingUrl = context.GetValue(SharingURL);
        }
        protected override async Task<Action<AsyncCodeActivityContext>> ExecuteAsyncWithClient(CancellationToken token, GraphServiceClient client)
        {
            var driveItemTask = client.GetDriveItemFromSharingUrl(token, sharingUrl);
            var listItemTask  = client.GetListItemFromSharingUrl(token, sharingUrl);
            var siteTask = client.GetSiteFromSharingUrl(token, sharingUrl);
            await Task.WhenAll(driveItemTask, listItemTask, siteTask);

            return ctx =>
            {
                ctx.SetValue(DriveItem, driveItemTask.Result);
                ctx.SetValue(ListItem, listItemTask.Result);
                ctx.SetValue(Site, siteTask.Result);
            };

        }
    }
}
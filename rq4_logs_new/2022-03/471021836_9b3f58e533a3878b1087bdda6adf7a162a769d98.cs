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
    [DisplayName("Get Sharepoint Information From Reference")]
    public class GetSharepointInformationFromReference : Office365Activity
    {
        [RequiredArgument]
        public InArgument<ItemReference> Reference { get; set; }
        public OutArgument<Drive> Drive { get; set; }
        public OutArgument<Site> Site { get; set; }
        public OutArgument<string> DriveName { get; set; }
        public OutArgument<string> SiteURL { get; set; }
        private ItemReference ReferenceValue { get; set; }
        protected override async Task<Action<AsyncCodeActivityContext>> ExecuteAsyncWithClient(CancellationToken token, GraphServiceClient client)
        {
            Site site = await client.GetSiteFromSiteId(token, ReferenceValue.SiteId);
            Drive drive = await client.GetDriveFromDriveId(token, site.Id, ReferenceValue.DriveId);
            return ctx =>
            {
                ctx.SetValue(Drive, drive);
                ctx.SetValue(Site, site);
                ctx.SetValue(DriveName, drive.Name);
                ctx.SetValue(SiteURL, site.WebUrl);
            };

        }
        protected override Task Initialize(GraphServiceClient client, AsyncCodeActivityContext context, CancellationToken token)
        {
            return Task.CompletedTask;
        }
        protected override void ReadContext(AsyncCodeActivityContext context)
        {
            ReferenceValue = Reference.Get(context);
        }
    }
}
using Microsoft.Graph;
using System;
using System.Activities;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Impower.Office365.Sharepoint.Activities
{
    [DisplayName("Update DriveItem Field")]
    public class UpdateDriveItemField : SharepointDriveItemActivity
    {
        [DisplayName("Field Name")]
        [RequiredArgument]
        [Category("Input")]
        public InArgument<string> FieldName { get; set; }
        [DisplayName("Value")]
        [RequiredArgument]
        [Category("Input")]
        public InArgument<object> Field { get; set; }
        protected string FieldNameValue;
        protected object FieldValue;
        protected Dictionary<string, object> fieldData = new Dictionary<string, object>();
        [DisplayName("Updated Fields")]
        [Category("Output")]
        public OutArgument<Dictionary<string,object>> UpdatedFields { get; set; }
        protected override void ReadContext(AsyncCodeActivityContext context)
        {
            base.ReadContext(context);
            FieldNameValue = FieldName.Get(context);
            FieldValue = Field.Get(context);
        }
        protected override async Task<Action<AsyncCodeActivityContext>> ExecuteAsyncWithClient(CancellationToken token, GraphServiceClient client)
        {
            var list = await client.GetSharepointList(token, SiteId, ListId);

            //TODO - this logic is messy - potential collisions of internal names and display names could lead to unexpected behavior.
            var writeableColumns = list.Columns.Where(column => !(column.ReadOnly ?? false));
            var matchingColumns = writeableColumns.Where(column => column.Name.Equals(FieldNameValue) || column.DisplayName.Equals(FieldNameValue));
            if (matchingColumns.Any())
            {
                fieldData[matchingColumns.First().Name] = FieldValue;
            }
            else
            {
                throw new Exception($"Could not find a field matching '{FieldNameValue}' in the target list. Available fields are: {String.Join(",", writeableColumns.Select(column => column.Name))}");
            }
            var updatedFields = await client.UpdateSharepointDriveItemFields(token, SiteId, DriveId, DriveItemIdValue, new FieldValueSet { AdditionalData = fieldData });
            return ctx =>
            {
                ctx.SetValue(UpdatedFields, updatedFields.AdditionalData);
            };
        }
    }
}
using Microsoft.SharePoint;
using Microsoft.SharePoint.WebControls;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Xnet.SP.MJU.Contents.FieldTypes
{
    public class FTFileUploader : SPFieldUrl
    {
        public FTFileUploader(SPFieldCollection fields, string fieldName): base(fields, fieldName)
        {
            this.DisplayFormat = SPUrlFieldFormatType.Hyperlink;
        }

        public FTFileUploader(SPFieldCollection fields, string typeName, string displayName) : base(fields, typeName, displayName)
        {
            this.DisplayFormat = SPUrlFieldFormatType.Hyperlink;
        }

        public override BaseFieldControl FieldRenderingControl
        {
            get
            {
                BaseFieldControl control = new FTFileUploaderControl();
                control.FieldName = this.InternalName;
                return control;
            }
        }

        public bool HideOOTBAttachFilesMenu
        {
            get
            {
                return (bool)this.GetCustomProperty("HideOOTBAttachFilesMenu");
            }
            set
            {
                this.SetCustomProperty("HideOOTBAttachFilesMenu", value);
            }
        }
    }
}
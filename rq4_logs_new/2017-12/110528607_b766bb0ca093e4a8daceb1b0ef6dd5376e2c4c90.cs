using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace CRM_GTMK.Visual.AddCompanyPanels.OfficesPanel.OneOfficePanel.OneOfficeContactTableLayoutPanel.OfficeNumberLabel
{
    public class MyOfficeNumberLabel : Label
    {
        public MyOfficeNumberLabel(AddNewCompanyForm form, MyOneOfficeContactTableLayoutPanel myOneOfficeContactTableLayoutPanel)
        {
            Label officeNumberLabel = form.GetOfficeNumberLabel();

            Anchor = officeNumberLabel.Anchor;
            SetSpans(form, officeNumberLabel, myOneOfficeContactTableLayoutPanel);
            SetFont(officeNumberLabel);
            Location = officeNumberLabel.Location;
            Name = officeNumberLabel.Name;
            Size = officeNumberLabel.Size;
            TabIndex = officeNumberLabel.TabIndex;
            Text = officeNumberLabel.Text + (1 + form.OfficeNumber);
            TextAlign = officeNumberLabel.TextAlign;
        }

        private void SetSpans(AddNewCompanyForm form, Label officeNumberLabel,
                              MyOneOfficeContactTableLayoutPanel myOneOfficeContactTableLayoutPanel)
        {
            int columnSpan = form.GetOneOfficeContactTableLayoutPanel().GetColumnSpan(officeNumberLabel);
            myOneOfficeContactTableLayoutPanel.SetColumnSpan(this, columnSpan);

            int rowSpan = form.GetOneOfficeContactTableLayoutPanel().GetRowSpan(officeNumberLabel);
            myOneOfficeContactTableLayoutPanel.SetRowSpan(this, rowSpan);
        }

        private void SetFont(Label officeNumberLabel)
        {
            string Name = officeNumberLabel.Font.FontFamily.Name;
            float emSize = officeNumberLabel.Font.SizeInPoints;
            FontStyle fontStyle = officeNumberLabel.Font.Style;
            GraphicsUnit graphicsUnit = officeNumberLabel.Font.Unit;
            Byte fontByte = officeNumberLabel.Font.GdiCharSet;
            Font = new Font(Name, emSize, fontStyle, graphicsUnit, ((byte)(fontByte)));
        }
    }
}
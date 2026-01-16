using CRM_GTMK.Visual.AddCompanyPanels.OfficesPanel.OneOfficePanel.ContactPersonFlowPanel.ContactPersonPanel.OneContactPersonPanel;
using CRM_GTMK.Visual.AddCompanyPanels.OfficesPanel.OneOfficePanel.OneOfficeContactTableLayoutPanel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace CRM_GTMK.Visual.AddCompanyPanels.OfficesPanel.OneOfficePanel.ContactPersonFlowPanel.ContactPersonPanel
{
    public class MyContactPersonPanel : Panel
    {
        public MyContactPersonPanel(AddNewCompanyForm form, MyOneOfficeContactTableLayoutPanel myOneOfficeContactTableLayoutPanel)
        {
            Panel panel = form.GetContactPersonPanel();

            Location = panel.Location;
            Name = panel.Name;
            Size = panel.Size;
            TabIndex = panel.TabIndex;

            MyContactPersonsLabel contactPersonsLabel = new MyContactPersonsLabel(form);
            MyContactPersonsButton contactPersonsButton = new MyContactPersonsButton(form);

            Controls.Add(contactPersonsLabel);
            Controls.Add(contactPersonsButton);

            SetSpans(form, panel, myOneOfficeContactTableLayoutPanel);
        }

        private void SetSpans(AddNewCompanyForm form, Panel panel, 
                              MyOneOfficeContactTableLayoutPanel myOneOfficeContactTableLayoutPanel)
        {
            int columnSpan = form.GetOneOfficeContactTableLayoutPanel().GetColumnSpan(panel);
            myOneOfficeContactTableLayoutPanel.SetColumnSpan(this, columnSpan);

            int rowSpan = form.GetOneOfficeContactTableLayoutPanel().GetRowSpan(panel);
            myOneOfficeContactTableLayoutPanel.SetRowSpan(this, rowSpan);
        }
    }
}
using CRM_GTMK.Visual.AddCompanyPanels.OfficesPanel.NewCompanyActionMenuPanel;
using CRM_GTMK.Visual.AddCompanyPanels.OfficesPanel.OneOfficePanel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace CRM_GTMK.Visual.AddCompanyPanels.OfficesPanel
{
	public class MyAllOfficesFlowLayoutPanel : FlowLayoutPanel
	{
        public MyAllOfficesFlowLayoutPanel(AddNewCompanyForm form)
        {
            FlowLayoutPanel allOfficesFlowLayoutPanel = form.GetAllOfficesFlowLayoutPanel();

            FlowDirection = allOfficesFlowLayoutPanel.FlowDirection;
            Location = allOfficesFlowLayoutPanel.Location;
            Name = allOfficesFlowLayoutPanel.Name;
            Size = allOfficesFlowLayoutPanel.Size;
            TabIndex = allOfficesFlowLayoutPanel.TabIndex;

            MyOneOfficeFlowLayoutPanel oneOfficeFlowLayoutPanel = new MyOneOfficeFlowLayoutPanel(form);
            MyNewCompanyActionMenuPanel newCompanyActionMenuPanel = new MyNewCompanyActionMenuPanel(form);

            Controls.Add(oneOfficeFlowLayoutPanel);
            Controls.Add(newCompanyActionMenuPanel);
        }
    }
}
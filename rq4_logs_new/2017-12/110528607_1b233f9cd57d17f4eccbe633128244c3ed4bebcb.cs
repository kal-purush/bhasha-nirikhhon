using CRM_GTMK.Visual.AddCompanyPanels.OfficesPanel.OneOfficePanel.GeneralContactInfoPanel.OfficeContactInfoPanel.OneOfficeContactInfoPanel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace CRM_GTMK.Visual.AddCompanyPanels.OfficesPanel.OneOfficePanel.GeneralContactInfoPanel.OfficeContactInfoPanel
{
    class OfficeContactInfoPanel : Panel
    {
        public TextBox OfficeAddressTextBox { get; set; }
        public TextBox OfficeCityTextBox { get; set; }
        public TextBox OfficeSiteTextBox { get; set; }
        public ComboBox OfficeCountryComboBox { get; set; }

        public OfficeContactInfoPanel(AddNewCompanyForm form)
        {
            Panel officeContactInfoPanel = form.GetOfficeContactInfoPanel();

            BorderStyle = officeContactInfoPanel.BorderStyle;
            Location = officeContactInfoPanel.Location;
            Name = officeContactInfoPanel.Name;
            Size = officeContactInfoPanel.Size;
            TabIndex = officeContactInfoPanel.TabIndex;

            OfficeAddressTextBox = new OfficeAddressTextBox(form);
            OfficeAddressLabel officeAddressLabel = new OfficeAddressLabel(form);
            OfficeCityTextBox = new OfficeCityTextBox(form);
            OfficeCityLabel officeCityLabel = new OfficeCityLabel(form);
            OfficeSiteTextBox = new OfficeSiteTextBox(form);
            OfficeSiteLabel officeSiteLabel = new OfficeSiteLabel(form);
            OfficeCountryComboBox = new OfficeCountryComboBox(form);
            OfficeCountryLabel officeCountryLabel = new OfficeCountryLabel(form);

            Controls.Add(OfficeAddressTextBox);
            Controls.Add(officeAddressLabel);
            Controls.Add(OfficeCityTextBox);
            Controls.Add(officeCityLabel);
            Controls.Add(OfficeSiteTextBox);
            Controls.Add(officeSiteLabel);
            Controls.Add(OfficeCountryComboBox);
            Controls.Add(officeCountryLabel);
        }
    }
}
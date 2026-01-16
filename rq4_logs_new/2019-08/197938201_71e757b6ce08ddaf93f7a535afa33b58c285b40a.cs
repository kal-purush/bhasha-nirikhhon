using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using UrlaubsPlaner.DBInteraction;
using UrlaubsPlaner.Entities;

namespace UrlaubsPlaner.Controller
{
    public class AbsenceType_FormController : IForm
    {
        private bool IsInsert = true;
        private List<AbsenceType> AbsenceTypes;
        private readonly AbsenceType_Form AbsenceType_Form;

        public AbsenceType_FormController(AbsenceType_Form absenceType_Form)
        {
            AbsenceType_Form = absenceType_Form;
            AbsenceType_Form.absenceTypeListView.SelectedIndexChanged += new System.EventHandler(this.AbsenceTypeListView_SelectedIndexChanged);
            AbsenceType_Form.createButton.Click += new System.EventHandler(this.CreateButton_Click);
            AbsenceType_Form.cancelBtn.Click += new System.EventHandler(this.CancelBtn_Click);
            AbsenceType_Form.btn_clear.Click += new System.EventHandler(this.Btn_clear_Click);
            AbsenceType_Form.Load += new System.EventHandler(this.AbsenceType_Form_Load);
        }

        private void CreateButton_Click(object sender, EventArgs e)
        {
            DataBaseConnection.UpsertAbsenceType(new AbsenceType() { AbsenceTypeId = AbsenceType_Form.txbx_id.Text != string.Empty ? new Guid(AbsenceType_Form.txbx_id.Text) : Guid.NewGuid(), Label = AbsenceType_Form.absenceType_Label.Text }, IsInsert);
            UpdataAbsenceTypes();
        }

        private void AbsenceType_Form_Load(object sender, EventArgs e)
        {
            UpdataAbsenceTypes();
        }

        private void UpdataAbsenceTypes()
        {
            AbsenceType_Form.absenceTypeListView.Items.Clear();
            AbsenceTypes = DataBaseConnection.GetAbsenceTypes();
            AbsenceType_Form.absenceTypeListView.Items.AddRange(AbsenceTypes.Select(x
                => new ListViewItem(new string[]
                {
                    x.AbsenceTypeId.ToString(),
                    x.Label
                })).ToArray());
        }

        private void CancelBtn_Click(object sender, EventArgs e)
        {
            AbsenceType_Form.Hide();
        }

        private void AbsenceTypeListView_SelectedIndexChanged(object sender, EventArgs e)
        {
            if (AbsenceType_Form.absenceTypeListView.SelectedIndices.Count == 1)
            {
                var index = AbsenceType_Form.absenceTypeListView.SelectedIndices[0];
                var listViewItem = AbsenceType_Form.absenceTypeListView.Items[index];
                AbsenceType_Form.absenceType_Label.Text = listViewItem.SubItems[1].Text;
                AbsenceType_Form.txbx_id.Text = listViewItem.SubItems[0].Text;
                ToggleInsertOrUpdate(true);
            }
        }

        private void ToggleInsertOrUpdate(bool visible)
        {
            IsInsert = !visible;
            ToggleButtonVisibility(visible);
            ChangeButtonText(!visible);

            if (!visible)
                ClearTextBoxes();
        }

        private void ToggleButtonVisibility(bool visible)
        {
            AbsenceType_Form.txbx_id.Visible = visible;
            AbsenceType_Form.lb_Id.Visible = visible;
            AbsenceType_Form.btn_clear.Visible = visible;
        }

        private void ClearTextBoxes()
        {
            AbsenceType_Form.txbx_id.Text = string.Empty;
            AbsenceType_Form.absenceType_Label.Text = string.Empty;
        }

        private void ChangeButtonText(bool isInsert)
        {
            if (isInsert)
            {
                AbsenceType_Form.createButton.Text = "Erstellen";
            }
            else
            {
                AbsenceType_Form.createButton.Text = "Aktualisieren";
            }
        }

        private void Btn_clear_Click(object sender, EventArgs e)
        {
            ToggleInsertOrUpdate(false);
        }

        public void ShowForm()
        {
            AbsenceType_Form.Show();
        }
    }
}
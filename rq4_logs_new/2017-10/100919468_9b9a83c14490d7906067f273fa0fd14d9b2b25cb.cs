using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using BussinessLayer;

namespace Library
{
    public partial class frmUserInfo : Form
    {
        public frmUserInfo ()
        {
            InitializeComponent();
        }

        private void _CloseButton_Click(object sender, EventArgs e)
        {
            this.Close();
        }
        BALMember balMember = new BALMember();
        private void frmUserInfo_Load(object sender, EventArgs e)
        {
            LoadCombobox();
        }

        private void LoadCombobox()
        {
            DataTable dt = new DataTable();
            DataTable dtnew = new DataTable();
            dt = balMember.GetMemberStatus();
            DataRow dr = dt.NewRow();
            dr["MStatusName"] = "--Please Select--";
            dr["MStatusID"] = 0;
            dt.Rows.InsertAt(dr, 0);
            cboStatus.DisplayMember = "MstatusName";
            cboStatus.ValueMember = "MStatusID";
            cboStatus.DataSource = dt;
        }

        private void txtUserName_TextChanged(object sender, EventArgs e)
        {

        }
    }
}
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Device_Managament2804
{
    public class ComboboxItem
    {
        public string Text { get; set; }
        public object Value { get; set; }

        public override string ToString()
        {
            return Text;
        }

        #region ////////////////////////////////////Load data to combobox////////////////////////////////////
        //Load dữ liệu lên combobox
        public void LoadDataToCombobox(ComboBox c, string sSQL, string DisplayMember, string ValueMember)
        {
            OleDbConnection odcConnect = new OleDbConnection(cls_KetNoi.ConnectionString_DE);
            OleDbCommand odcCommand = new OleDbCommand(sSQL, odcConnect);

            odcConnect.Open();
            OleDbDataReader odrReader = odcCommand.ExecuteReader();
            c.Items.Clear();

            ComboboxItem cbo;
            try
            {
                while (odrReader.Read())
                {
                    //Khởi tạo biến ComboboxItem để thêm vào Combobox
                    cbo = new ComboboxItem();
                    cbo.Text = odrReader[DisplayMember].ToString();
                    cbo.Value = odrReader[ValueMember].ToString();
                    //Thêm comboboxItem vào Combobox
                    c.Items.Add(cbo);
                }
            }
            catch (Exception exMsg)
            {
                if (exMsg != null)
                {
                    if (odrReader != null) odrReader.Dispose();
                }
            }
            finally
            {
                if (odrReader != null) odrReader.Dispose();
            }
        }
        #endregion
    }
}
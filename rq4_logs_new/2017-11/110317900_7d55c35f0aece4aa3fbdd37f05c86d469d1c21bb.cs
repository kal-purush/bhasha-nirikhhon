using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace RUP
{
    public partial class frmLogin : Form
    {
        public frmLogin()
        {
            InitializeComponent();
        }
        public static string TenTaiKhoan="";
        Bo.DangNhapBo dangnhapbo = new Bo.DangNhapBo();
        private void Login_Load(object sender, EventArgs e)
        {
            //txtTen.Text = "";
            //txtMatKhau.Text = "";
        }

        private void buttonX1_Click(object sender, EventArgs e)
        {
            bool kTraDangNhap = dangnhapbo.kTraDangNhap(txtTen.Text, txtMatKhau.Text);
            if (kTraDangNhap)
            {
                TenTaiKhoan = txtTen.Text;
                new frmThayDoiMatKhau().ShowDialog();
            }
            else
            {
                
                if (txtTen.Text.Equals(""))
                {
                    txtLoiTen.Text = "Nhập tên";
                }
                else
                {

                    txtLoiTen.Text = "";
                }
                if (txtMatKhau.Text.Equals(""))
                {
                    txtLoiMatKhau.Text = "Nhập Mật Khẩu";
                }
                else
                {
                    txtLoiMatKhau.Text = "";
                }
                if (!txtTen.Text.Equals("")&&!txtMatKhau.Equals(""))
                {
                    MessageBox.Show("Tài khoản không tồn tại");
                }
            }
            txtTen.ForeColor = Color.Black;
            txtMatKhau.ForeColor = Color.Black;
            txtMatKhau.PasswordChar = '*';
        }

        private void buttonX2_Click(object sender, EventArgs e)
        {
            this.Close();
        }
    }
}
using MyHabit.DTO;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace MyHabit
{
    public partial class FrmforgetPassword : Form
    {
        public FrmforgetPassword()
        {
            InitializeComponent();
        }
        static string mess1;
        static string mess2;
        public static async Task<String> PostTK(string mail, string pass)
        {
            HttpClient client = new HttpClient();
            client.BaseAddress = new Uri("http://61.14.233.182");
            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Accept.Add(
                new MediaTypeWithQualityHeaderValue("application/json"));
            // bật cái dưới khi gọi api khác trừ login register forget password
            /*client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", "token");*/

            // Link api
            String apiUrl = "/api/auth/forget-password";

            // Model  để gửi dữ liệu lên server
            ChangeAcc acount = new ChangeAcc();
            acount.email = mail;
            acount.password = pass;



            // POST dùng postAsJsonAsync có body
            // Get dùng GetAsync ko có body
            // PUT dùng PutAsJsonAsync có body
            // Delete dùng DeleteAsync ko có body
            HttpResponseMessage response = await client.PostAsJsonAsync(apiUrl, acount); // login là body của api

            // Kiểm tra api có thành công hay ko
            if (response.IsSuccessStatusCode)
            {
                // nếu server trả về 1 chuổi thì mình dùng như cách bên dưới
                // server trả về một đối tượng thì dùng await response.Content.ReadAsAsync<Doi tuong>()
                var token = await response.Content.ReadAsStringAsync();
                mess1 = token;
                return token;
            }
            else
            {
                var token = await response.Content.ReadAsStringAsync();
                mess2 = token;
                return null;
            }
        }
        bool checkThongTin()
        {
            

           if (tbPass.Text != tbRePass.Text)
            {
                // 2 password ko khớp sẽ không cho đk và trả về false
                MessageBox.Show("mật khẩu không khớp nhau", "kiểm tra lại mật khẩu", MessageBoxButtons.OK);
                return false;
            }
            //thoả đk sẽ gọi hàm đăng ký
            else return true;
        }

        private async void button2_Click(object sender, EventArgs e)
        {
            var token = PostTK(tbName.Text, tbPass.Text);
            
            if (checkThongTin() == true && token!= null)
            {
                MessageBox.Show(mess1);
            }
            else
                MessageBox.Show(mess2);
        }

        private void btBack_Click(object sender, EventArgs e)
        {
            FrmLogin frmLogin = new FrmLogin();
            this.Hide();
            frmLogin.ShowDialog();
        }
    }
}
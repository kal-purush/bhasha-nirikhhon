using System.Net.Mail;
using System.Text.RegularExpressions;
        AirEntities dbContext = new AirEntities();


        private void phone_txt_PreviewTextInput(object sender, TextCompositionEventArgs e)
        {
            var txtOld = phone_txt.Text;
            var txtNew = e.Text;
            if (!Regex.IsMatch(txtNew, "[0-9]"))
            {
                e.Handled = true; // 敲不下去
            }
            if (txtOld.Length == 10)
            {
                e.Handled = true; // 敲不下去
            }
        }

        private void RegisterBtn_Click(object sender, RoutedEventArgs e)
        {
            if (firstname_txt.Text == "")
            {
                firstname_txt.BorderBrush = Brushes.Red;
            }
            else
            {
                firstname_txt.BorderBrush = Brushes.LightGray;  //不是預設值
            }

            if (lastname_txt.Text == "")
            {
                lastname_txt.BorderBrush = Brushes.Red;
            }
            else
            {
                lastname_txt.BorderBrush = Brushes.LightGray;
            }

            if (password_txt.Password == "")
            {
                password_txt.BorderBrush = Brushes.Red;
            }
            else
            {
                password_txt.BorderBrush = Brushes.LightGray;
            }

            if (phone_txt.Text == "")
            {
                phone_txt.BorderBrush = Brushes.Red;
            }
            else
            {
                phone_txt.BorderBrush = Brushes.LightGray;
            }

            if (email_txt.Text == "")
            {
                email_txt.BorderBrush = Brushes.Red;
            }
            else
            {
                email_txt.BorderBrush = Brushes.LightGray;
            }

            string FirstName = this.firstname_txt.Text;
            string LastName = this.lastname_txt.Text;
            string Password = this.password_txt.Password;   //***
            string Phone = this.phone_txt.Text;
            string Email = this.email_txt.Text;

            //Password = HashPasswordForStoringInConfigFile(Password + salt, "sha1");


            Member newMember = new Member();
            newMember.Member_En_FirstName = FirstName.ToUpper();
            newMember.Member_En_LastName = LastName.ToUpper();
            newMember.Member_Password = Password;   //***
            newMember.Member_Phone = Phone;

            Regex regex = new Regex(@"^([\w\.\-]+)@([\w\-]+)((\.(\w){2,3})+)$");    //判斷是否為正確的e-mail
            Match match = regex.Match(Email);
            if (match.Success)
            {
                newMember.Member_Account = Email;
            }
            else
            {
                MessageBox.Show("請輸入正確的e-mail");
                return;
            }

            if (GenderCombo.SelectedIndex == 0)
            {
                newMember.Member_Gender = "男";
            }
            else if (GenderCombo.SelectedIndex == 1)
            {
                newMember.Member_Gender = "女";
            }
            else
            {
                MessageBox.Show("請選擇性別");
                return;     //就不會往下走
            }

            var isEmailExist = dbContext.Members.Any(x => x.Member_Account == Email);   //用any判斷email是否存在，無論大小寫
            if (isEmailExist)
            {
                MessageBox.Show("此帳號已有人註冊");
                return;
            }

            MailMessage msg = new MailMessage();
            //收件者，以逗號分隔不同收件者 ex "test@gmail.com,test2@gmail.com"
            //msg.To.Add(string.Join("email", MailList.ToArray()));
            msg.From = new MailAddress("msit120120@gmail.com", "資策會", Encoding.UTF8);
            msg.To.Add(Email);
            //郵件標題 
            msg.Subject = "airticket";
            //郵件標題編碼  
            msg.SubjectEncoding = System.Text.Encoding.UTF8;
            //郵件內容
            msg.Body = "<p style=\"color:red\">歡迎您加入會員</p>";
            msg.IsBodyHtml = true;
            msg.BodyEncoding = Encoding.UTF8;       //郵件內容編碼 
            msg.Priority = MailPriority.Normal;     //郵件優先級 
                                                    //建立 SmtpClient 物件 並設定 Gmail的smtp主機及Port 
            #region 其它 Host
            /*
             *  outlook.com smtp.live.com port:25
             *  yahoo smtp.mail.yahoo.com.tw port:465
            */
            #endregion
            SmtpClient MySmtp = new SmtpClient("smtp.gmail.com", 587);
            //設定你的帳號密碼
            MySmtp.Credentials = new System.Net.NetworkCredential("msit120120@gmail.com", "mmsit120");  //正常要加密
            //Gmial 的 smtp 使用 SSL
            MySmtp.EnableSsl = true;
            MySmtp.Send(msg);
            //啟用 低安全性應用程式存取權https://myaccount.google.com/lesssecureapps



            dbContext.Members.Add(newMember);
            dbContext.SaveChanges();
            MessageBox.Show("加入成功");

            ((LogInWindow)window).Tabs.SelectedIndex = 0;
        }
﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;

namespace AirTicket
{
    /// <summary>
    /// 會員管理.xaml 的互動邏輯
    /// </summary>
    public partial class 會員管理
    {
        public 會員管理()
        {
            InitializeComponent();
            //datagrid1.FrozenColumnCount = 1;
        }

        AirEntities dbContext = new AirEntities();
        // 會員管理
        private void Button_Click(object sender, RoutedEventArgs e)
        {
            //var result = dbContext.Members.Where(x => x.Member_Account == txt.Text ||
            //x.Member_Ch_FirstName == txt.Text || x.Member_Ch_LastName == txt.Text ||
            //x.Member_En_FirstName == txt.Text || x.Member_En_LastName == txt.Text ||
            //x.Member_Gender == txt.Text || x.Member_Phone == txt.Text);
            var result = dbContext.Members.AsEnumerable().Where(x => x.Member_Account == email_txt.Text).Select(x => new
            {
                E_mail = x.Member_Account,
                英文_姓 = x.Member_En_FirstName,
                英文_名 = x.Member_En_LastName,
                中文_姓 = x.Member_Ch_FirstName,
                中文_名 = x.Member_Ch_LastName,
                性別 = x.Member_Gender,
                生日 = x.Date_Of_Birth?.ToShortDateString(),
                手機 = x.Member_Phone
            }); 
            datagrid1.ItemsSource = result.ToList();
        }

        private void Button_Click_1(object sender, RoutedEventArgs e)
        {
            var res = dbContext.Members.FirstOrDefault(x => x.Member_Account == email_txt.Text);
            if (res != null)
            {
                res.Member_Ch_FirstName = cnFirst_txt.Text;
                res.Member_Ch_LastName = cnLast_txt.Text;
                res.Date_Of_Birth = birth_date.SelectedDate;    //會顯示時間
                res.Member_Phone = phone_txt.Text;
                dbContext.SaveChanges();
                MessageBox.Show("更新成功");
            }
            else
            {
                email_txt.BorderBrush = Brushes.Red;
                MessageBox.Show("請填寫正確E-Mail");
            }
        }

        private void datagrid1_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            //datagrid1.FrozenColumnCount = 1;
        }

        private void Button_Click_2(object sender, RoutedEventArgs e)
        {
            try
            {
                var res = dbContext.Members.First(x => x.Member_Account == email_txt.Text);
                dbContext.Members.Remove(res);
                dbContext.SaveChanges();
                MessageBox.Show("刪除成功");
            }
            catch
            {
                email_txt.BorderBrush = Brushes.Red;
                MessageBox.Show("請填寫正確E-Mail");
            }
        }

        private void Button_Click_3(object sender, RoutedEventArgs e)   //會員管理
        {
            //int a = 5;
            //int? b = 5;
            //b = null;
            var result = dbContext.Members.AsEnumerable().Select(x => new
            {
                E_mail = x.Member_Account,
                英文_姓 = x.Member_En_FirstName,
                英文_名 = x.Member_En_LastName,
                中文_姓 = x.Member_Ch_FirstName,
                中文_名 = x.Member_Ch_LastName,
                性別 = x.Member_Gender,
                生日 = x.Date_Of_Birth?.ToShortDateString(),  //DateTime"?"可用於nullable物件 空值，只顯示日期
                手機 = x.Member_Phone
            });
            datagrid1.ItemsSource = result.ToList();
        }

        private void Button_Click_5(object sender, RoutedEventArgs e)
        {
            email_txt.Text = "547@gmail.com";
            cnFirst_txt.Text = "無";
            cnLast_txt.Text = "奇隆";
            phone_txt.Text = "0952120120";
        }
    }
}
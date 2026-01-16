using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.IO;

namespace Tests
{
    public partial class Form2 : Form
    {

        string[] str = null;
        int i = 0;
        int str_length = 0;

        public Form2()
        {
            InitializeComponent();

            comboBox1.Items.Add("C++");
            comboBox1.Items.Add("C#");
            comboBox1.Items.Add("SQL");

        
        }
      


        private void button1_Click(object sender, EventArgs e)
        {

            if (str == null)
            {
                MessageBox.Show("Choose the subject!!");    
                return;
            }
            button1.Text = "Next";
            if (i == str_length) {
                MessageBox.Show("End");
                button1.Enabled = false;
                return;
            }

                    listBox1.Items.Clear();
                    listBox1.Items.Add(str[i++]);   //додає питання

                    radioButton1.Text = str[i++];   //відповіді
                    radioButton2.Text = str[i++];
                    radioButton3.Text = str[i++];

                    if (radioButton1.Text.Contains("+")) {

                        radioButton1.Tag = true;    ////мітка для правильного питання
                        int s = radioButton1.Text.IndexOf("+");
                        string st = radioButton1.Text;

                       st = st.Remove(s);   // видаляє помітку з правильного питання(тобто плюс зі стрічки)
                       radioButton1.Text = st;
                    }
                    if (radioButton2.Text.Contains("+"))
                    {

                        radioButton2.Tag = true;
                        int s = radioButton2.Text.IndexOf("+");
                        string st = radioButton2.Text;

                        st = st.Remove(s);
                        radioButton2.Text = st;
                    }
                    if (radioButton3.Text.Contains("+"))
                    {

                        radioButton3.Tag = true;
                        int s = radioButton3.Text.IndexOf("+");
                        string st = radioButton3.Text;

                        st = st.Remove(s);
                        radioButton3.Text = st;
                    }
     
        }

        private void comboBox1_SelectedIndexChanged(object sender, EventArgs e)
        {
            i = 0;
            string[] lines = null;
            if (comboBox1.SelectedIndex == 0)
            {
                lines = File.ReadAllLines(@"C:\Users\Dima\Desktop\C++test.txt");   // файл txt
               str_length = lines.Length;

            }
            if (comboBox1.SelectedIndex == 1)
            {
                lines = File.ReadAllLines(@"C:\Users\Dima\Desktop\C#test.txt");
                str_length = lines.Length;
            }
            if (comboBox1.SelectedIndex == 2)
            {
                lines = File.ReadAllLines(@"C:\Users\Dima\Desktop\Sqltest.txt");
                str_length = lines.Length;
            }
             str = lines;

        }
    }
}
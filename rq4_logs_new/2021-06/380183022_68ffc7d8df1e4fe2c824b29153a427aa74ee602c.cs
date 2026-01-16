using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace GissaNummer
{
    public partial class Form1 : Form
    {
        public string[] svar = new string[4];
        public string[] gissning = new string[4] {"0", "0", "0", "0"};
        public int nr = 0;

        public Form1()
        {
            InitializeComponent();
            RättSvar(svar);
        }

        public string[] RättSvar(string[] x)
        {
            Random rnd = new Random();
            int[] y = new int[4];
            for(int i = 0; i <= 3; i++)
            {
                y[i] = rnd.Next(1, 10);
                for (int j = 0; j < i; j++)
                {
                    if(y[j] == y[i])
                    {
                        i--;
                        break;
                    }
                }
            }
            foreach(int num in y)
            {
                System.Diagnostics.Debug.WriteLine(num);
            }
            for(int i = 0; i < 4; i++)
            {
                x[i] = y[i].ToString();
            }
            return x;
        }

        public void Rätta(string[] x)
        {
            int num = 0;
            int plats = 0;
            int varv = 0;

            foreach(string siffra in x)
            {
                for(int i = 0; i < 4; i++)
                {
                    if(siffra == svar[i] && varv == i)
                    {
                        plats++;
                        break;
                    }
                    else if(siffra == svar[i])
                    {
                        num++;
                        break;
                    }
                }
                varv++;
            }
            if(plats != 4)
            {
                ClueText.Text = $"{plats} Correct In The Right Place\r\n{num} Correct In The Wrong Place";
                buttonGuess.Text = "RESET";
                buttonGuess.Click -= buttonGuess_Click;
                buttonGuess.Click += buttonGuess_Click2;
            }
            else if(plats == 4)
            {
                ClueText.Text = UserInput.Text + " Was Correct!\r\nCongratulations!";
                UserInput.Text = "WIN!";
                buttonGuess.Text = "AGAIN?";
                buttonGuess.Click -= buttonGuess_Click;
                buttonGuess.Click += buttonGuess_Click3;
            }
            foreach(string str in svar)
            {
                System.Diagnostics.Debug.WriteLine(str);
            }
            foreach (string str in gissning)
            {
                System.Diagnostics.Debug.WriteLine(str);
            }
            System.Diagnostics.Debug.WriteLine(plats);
            System.Diagnostics.Debug.WriteLine(num);
            System.Diagnostics.Debug.WriteLine(varv);
        }

        private void Form1_Load(object sender, EventArgs e)
        {

        }

        private void button2_Click(object sender, EventArgs e)
        {
            gissning[nr] = "2";
            UserInput.Text = String.Concat(gissning);
            nr++;
            if (nr == 4)
            {
                buttonGuess.Text = "Guess";
                buttonGuess.Visible = true;
            }
            button2.Enabled = false;
        }

        private void panel1_Paint(object sender, PaintEventArgs e)
        {

        }

        private void button2_Click_1(object sender, EventArgs e)
        {
            gissning[nr] = "5";
            UserInput.Text = String.Concat(gissning);
            nr++;
            if (nr == 4)
            {
                buttonGuess.Text = "Guess";
                buttonGuess.Visible = true;
                foreach (Control ctrl in panel1.Controls)
                {
                    ctrl.Enabled = false;
                }
            }
            button5.Enabled = false;
        }

        private void button2_Click_2(object sender, EventArgs e)
        {
            gissning[nr] = "3";
            UserInput.Text = String.Concat(gissning);
            nr++;
            if (nr == 4)
            {
                buttonGuess.Text = "Guess";
                buttonGuess.Visible = true;
                foreach (Control ctrl in panel1.Controls)
                {
                    ctrl.Enabled = false;
                }
            }
            button3.Enabled = false;
        }

        private void button1_Click(object sender, EventArgs e)
        {
            gissning[nr] = "1";
            UserInput.Text = String.Concat(gissning);
            nr++;
            if(nr == 4)
            {
                buttonGuess.Text = "Guess";
                buttonGuess.Visible = true;
                foreach (Control ctrl in panel1.Controls)
                {
                    ctrl.Enabled = false;
                }
            }
            button1.Enabled = false;
        }

        private void buttonGuess_Click(object sender, EventArgs e)
        {
            Rätta(gissning);
        }

        private void UserInput_MouseDown(object sender, MouseEventArgs e)
        {
            ActiveControl = button1;
        }

        private void UserInput_MouseEnter(object sender, EventArgs e)
        {

        }

        private void button4_Click(object sender, EventArgs e)
        {
            gissning[nr] = "4";
            UserInput.Text = String.Concat(gissning);
            nr++;
            if (nr == 4)
            {
                buttonGuess.Text = "Guess";
                buttonGuess.Visible = true;
                foreach (Control ctrl in panel1.Controls)
                {
                    ctrl.Enabled = false;
                }
            }
            button4.Enabled = false;
        }

        private void button6_Click(object sender, EventArgs e)
        {
            gissning[nr] = "6";
            UserInput.Text = String.Concat(gissning);
            nr++;
            if (nr == 4)
            {
                buttonGuess.Text = "Guess";
                buttonGuess.Visible = true;
                foreach (Control ctrl in panel1.Controls)
                {
                    ctrl.Enabled = false;
                }
            }
            button6.Enabled = false;
        }

        private void button7_Click(object sender, EventArgs e)
        {
            gissning[nr] = "7";
            UserInput.Text = String.Concat(gissning);
            nr++;
            if (nr == 4)
            {
                buttonGuess.Text = "Guess";
                buttonGuess.Visible = true;
                foreach (Control ctrl in panel1.Controls)
                {
                    ctrl.Enabled = false;
                }
            }
            button7.Enabled = false;
        }

        private void button8_Click(object sender, EventArgs e)
        {
            gissning[nr] = "8";
            UserInput.Text = String.Concat(gissning);
            nr++;
            if (nr == 4)
            {
                buttonGuess.Text = "Guess";
                buttonGuess.Visible = true;
                foreach (Control ctrl in panel1.Controls)
                {
                    ctrl.Enabled = false;
                }
            }
            button8.Enabled = false;
        }

        private void button9_Click(object sender, EventArgs e)
        {
            gissning[nr] = "9";
            UserInput.Text = String.Concat(gissning);
            nr++;
            if (nr == 4)
            {
                buttonGuess.Text = "Guess";
                buttonGuess.Visible = true;
                foreach (Control ctrl in panel1.Controls)
                {
                    ctrl.Enabled = false;
                }
            }
            button9.Enabled = false;
        }

        private void UserInput_TextChanged(object sender, EventArgs e)
        {

        }

        private void buttonGuess_Click2(object sender, EventArgs e)
        {
            foreach(Control ctrl in panel1.Controls)
            {
                ctrl.Enabled = true;
            }
            ClueText.Text += $"\r\nLast Guess Was { UserInput.Text}";
            UserInput.Text = "0000";
            buttonGuess.Click -= buttonGuess_Click2;
            buttonGuess.Click += buttonGuess_Click;
            buttonGuess.Visible = false;
            for (int i = 0; i < 4; i++)
            {
                gissning[i] = "0";
            }
            this.nr = 0;
        }

        private void buttonGuess_Click3(object sender, EventArgs e)
        {
            foreach (Control ctrl in panel1.Controls)
            {
                ctrl.Enabled = true;
            }
            buttonGuess.Click -= buttonGuess_Click3;
            buttonGuess.Click += buttonGuess_Click;
            buttonGuess.Visible = false;
            ClueText.Clear();
            this.nr = 0;
            UserInput.Text = "0000";
            for(int i = 0; i < 4; i++)
            {
                gissning[i] = "0";
            }
            RättSvar(svar);
        }
    }
}
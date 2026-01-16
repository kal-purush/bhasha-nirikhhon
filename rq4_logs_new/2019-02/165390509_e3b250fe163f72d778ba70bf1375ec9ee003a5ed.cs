using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Drawing.Imaging;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;


namespace WindowsFormsApplication1
{
    public partial class ButtonDefaultForm : Form
    {
        public ButtonDefaultForm()
        {
            InitializeComponent();
        }

        private void Form2_Load(object sender, EventArgs e)
        {
            MainForm.pic(this);
        }

        private void pictureBox1_Click(object sender, EventArgs e)
        {
            DesignClass.BUTTON_BACKGROUND_IMG = ((PictureBox)sender).BackgroundImage;
            this.Close();
           // DesignClass.BUTTON_BACKGROUND_IMG_ADRESS = ((PictureBox)sender).Tag.ToString();
          
            MainForm.pic(this);
        }

        private void button1_Click(object sender, EventArgs e)
        {
            ColorDialog MyDialog = new ColorDialog();
            MyDialog.ShowDialog();

            this.ForeColor = MyDialog.Color;
            DesignClass.BUTTON_TEXT_COLOR = MyDialog.Color;
        }

        private void button2_Click(object sender, EventArgs e)
        {
            PicForm form = new PicForm();
            form.ShowDialog();

            Size pic1Size = new Size(pictureBox1.Size.Width, 0);
            Size pic2Size = new Size(pictureBox2.Size.Width, 0);
            Size s1 = new System.Drawing.Size(DesignClass.LENGTH, 0);
            pictureBox2.Location = pictureBox1.Location + pic1Size + s1;
            pictureBox3.Location = pictureBox2.Location + pic2Size + s1;
            MainForm.pic(this);
        }

        private void button3_Click(object sender, EventArgs e)
        {
            try
            {
                pictureBox4.Load(textBox1.Text);
            }
            catch (Exception)
            {
                pictureBox4.Image = pictureBox1.Image;
            }

            DesignClass.BUTTON_BACKGROUND_IMG = pictureBox4.Image;
        }

        private void button4_Click(object sender, EventArgs e)
        {
            if (fontDialog1.ShowDialog() != DialogResult.Cancel)
            {   
                if (fontDialog1.Font.Size > 5 && fontDialog1.Font.Size < 38)
                {
                    DesignClass.BUTTON_FONT = fontDialog1.Font;
                    MainForm.pic(this);
                }
            }
        }
    }
}
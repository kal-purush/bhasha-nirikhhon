using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using Model;

namespace Model_View
{
    
    public partial class New_Figures : Form
    {

        private byte _figure;

        public IFigure Figure { get; set; }

        public New_Figures()
        {
            InitializeComponent();
        }

        private void New_Figures_Load(object sender, EventArgs e)
        {

        }

        private void Cancle_Click(object sender, EventArgs e)
        {
            DialogResult = DialogResult.Cancel;
            this.Close();
        }



        private void Calculate_Click(object sender, EventArgs e)
        {
            Model.IFigure figureModel = null; 
            switch (_figure) 
            {
                case 0: 
                    figureModel = new ball(Convert.ToInt32(Radius.Text));
                    break;
                case 1: 
                    figureModel = new Parallelepiped(Convert.ToInt32(Aside.Text), Convert.ToInt32(Bside.Text), Convert.ToInt32(Cside.Text));
                    break;
                case 2: 
                    figureModel = new Piramide(Convert.ToInt32(H.Text), Convert.ToInt32(S.Text));
                    break;
            }
            Figure = figureModel;    
            DialogResult = DialogResult.OK;
            this.Close();
        }
          
        private void sphere_CheckedChanged(object sender, EventArgs e)
        {
            Aside.Visible = false;
            Bside.Visible = false;
            Cside.Visible = false;
            H.Visible = false;
            S.Visible = false;
            Radius.Visible = true;
            _figure = 0;
         }

    
        private void Cside_CheckedChanged(object sender, EventArgs e)
        {

        }

        private void listBox1_SelectedIndexChanged(object sender, EventArgs e)
        {

        }

        private void Aside_CheckedChanged_1(object sender, EventArgs e)
        {
            
        }

        private void Aside_Click(object sender, EventArgs e)
        {
         
        }

        private void textBox1_TextChanged(object sender, EventArgs e)
        {
            
        }

        private void parallelepiped_CheckedChanged_1(object sender, EventArgs e)
        {
            Aside.Visible = true;
            Bside.Visible = true;
            Cside.Visible = true;
            H.Visible = false;
            S.Visible = false;
            Radius.Visible = false;
            _figure = 1;
        }

        private void piramid_CheckedChanged_1(object sender, EventArgs e)
        {
            Aside.Visible = false;
            Bside.Visible = false;
            Cside.Visible = false;
            H.Visible = true;
            S.Visible = true;
            Radius.Visible = false;
            _figure = 2;
        }

        private void groupBox1_Enter(object sender, EventArgs e)
        {

        }

        private void Aside_KeyPress(object sender, KeyPressEventArgs e)
        {
            if ((e.KeyChar <= 47 || e.KeyChar >= 58) && e.KeyChar != 8)
                e.Handled = true;
        }

        private void Bside_KeyPress(object sender, KeyPressEventArgs e)
        {
            if ((e.KeyChar <= 47 || e.KeyChar >= 58) && e.KeyChar != 8)
                e.Handled = true;
        }

        private void Cside_KeyPress(object sender, KeyPressEventArgs e)
        {
            if ((e.KeyChar <= 47 || e.KeyChar >= 58) && e.KeyChar != 8)
                e.Handled = true;
        }

        private void H_KeyPress(object sender, KeyPressEventArgs e)
        {
            if ((e.KeyChar <= 47 || e.KeyChar >= 58) && e.KeyChar != 8)
                e.Handled = true;
        }

        private void S_KeyPress(object sender, KeyPressEventArgs e)
        {
            if ((e.KeyChar <= 47 || e.KeyChar >= 58) && e.KeyChar != 8)
                e.Handled = true;
        }

        private void Radius_KeyPress(object sender, KeyPressEventArgs e)
        {
            if ((e.KeyChar <= 47 || e.KeyChar >= 58) && e.KeyChar != 8)
                e.Handled = true;
        }
    }
}
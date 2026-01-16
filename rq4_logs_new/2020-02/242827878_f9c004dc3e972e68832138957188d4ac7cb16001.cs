using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using Wumpus_Test;

namespace WumpusTest
{
    public partial class Form1 : Form
    {
        private GameControl gameControl;
        
        public Form1()
        {
            InitializeComponent();

        }
        private void button2_Click(object sender, EventArgs e)
        {
            
            gameControl = new GameControl();
            MessageBox.Show("The game control class has been constructed!");
            ConstructorButton.Enabled = false;
        }

        private void button2_Click_1(object sender, EventArgs e)
        {
            gameControl.newGame();
            MessageBox.Show("GameControl has begun new game\n Welcome to Hunt The Wumpus: Space Edition!", "Message:");
            NewGameButton.Enabled = false;

        }
    }


    }

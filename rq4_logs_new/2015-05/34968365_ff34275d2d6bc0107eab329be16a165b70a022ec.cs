using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace AA
{
    public partial class Game : Form
    {
        Rotator Rotator;
        StartScreen parentForm;
        Timer closeTimer;

        public Game(StartScreen parent)
        {
            InitializeComponent();
            DoubleBuffered = true;
            Rotator = new Rotator(1);
            parentForm = parent;
            //FormBorderStyle = FormBorderStyle.None;
            //WindowState = FormWindowState.Maximized;

            KeyDown += Game_KeyDown;

            for (int i = 0; i < 10; i++)
            {
                Rotator.Balls.Add(new Ball(i * (360 / 10)));
            }
        }

        private void Game_Paint(object sender, PaintEventArgs e)
        {
            Rotator.Draw(e.Graphics);
        }

        private void timer1_Tick(object sender, EventArgs e)
        {
            Rotator.Rotate();
            Invalidate();
        }

        private void Game_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Space)
            {
                Rotator.AddNewBall();
                if (Rotator.CheckForCollision())
                {
                    LoseGame();
                    Invalidate();
                }
            }
        }

        private void LoseGame()
        {
            timer1.Stop();
            this.BackColor = Color.Red;
            Invalidate();
            FadeOutAndClose();
        }

        private void FadeOutAndClose()
        {
            closeTimer = new Timer();
            closeTimer.Interval = 50;
            closeTimer.Tick += fadeOutTick;
            closeTimer.Start();
        }

        private void fadeOutTick(object sender, EventArgs e)
        {
            this.Opacity -= 0.05;
            if (this.Opacity <= 0.05)
            {
                this.Close();
            }
        }

        private void Game_FormClosing(object sender, FormClosingEventArgs e)
        {
            parentForm.reloadForm();
        }
    }
}
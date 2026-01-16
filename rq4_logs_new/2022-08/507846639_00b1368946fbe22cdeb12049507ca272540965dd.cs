
namespace igra1
{
    partial class Form1
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            this.pipeTop = new System.Windows.Forms.PictureBox();
            this.HappyBirrd = new System.Windows.Forms.PictureBox();
            this.pipeBotton = new System.Windows.Forms.PictureBox();
            this.ground = new System.Windows.Forms.PictureBox();
            this.scoreText = new System.Windows.Forms.Label();
            this.gameTimer = new System.Windows.Forms.Timer(this.components);
            ((System.ComponentModel.ISupportInitialize)(this.pipeTop)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.HappyBirrd)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.pipeBotton)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ground)).BeginInit();
            this.SuspendLayout();
            // 
            // pipeTop
            // 
            this.pipeTop.Image = global::igra1.Properties.Resources.pipedown;
            this.pipeTop.Location = new System.Drawing.Point(476, -4);
            this.pipeTop.Name = "pipeTop";
            this.pipeTop.Size = new System.Drawing.Size(88, 101);
            this.pipeTop.SizeMode = System.Windows.Forms.PictureBoxSizeMode.StretchImage;
            this.pipeTop.TabIndex = 0;
            this.pipeTop.TabStop = false;
            // 
            // HappyBirrd
            // 
            this.HappyBirrd.Image = global::igra1.Properties.Resources.bird;
            this.HappyBirrd.Location = new System.Drawing.Point(215, 175);
            this.HappyBirrd.Name = "HappyBirrd";
            this.HappyBirrd.Size = new System.Drawing.Size(55, 48);
            this.HappyBirrd.SizeMode = System.Windows.Forms.PictureBoxSizeMode.StretchImage;
            this.HappyBirrd.TabIndex = 1;
            this.HappyBirrd.TabStop = false;
            this.HappyBirrd.Click += new System.EventHandler(this.HappyBirrd_Click);
            // 
            // pipeBotton
            // 
            this.pipeBotton.Image = global::igra1.Properties.Resources.pipe;
            this.pipeBotton.Location = new System.Drawing.Point(424, 351);
            this.pipeBotton.Name = "pipeBotton";
            this.pipeBotton.Size = new System.Drawing.Size(85, 122);
            this.pipeBotton.SizeMode = System.Windows.Forms.PictureBoxSizeMode.StretchImage;
            this.pipeBotton.TabIndex = 2;
            this.pipeBotton.TabStop = false;
            // 
            // ground
            // 
            this.ground.Image = global::igra1.Properties.Resources.ground;
            this.ground.Location = new System.Drawing.Point(2, 463);
            this.ground.Name = "ground";
            this.ground.Size = new System.Drawing.Size(597, 183);
            this.ground.SizeMode = System.Windows.Forms.PictureBoxSizeMode.StretchImage;
            this.ground.TabIndex = 3;
            this.ground.TabStop = false;
            // 
            // scoreText
            // 
            this.scoreText.AutoSize = true;
            this.scoreText.BackColor = System.Drawing.Color.PapayaWhip;
            this.scoreText.Font = new System.Drawing.Font("Arial", 26F, System.Drawing.FontStyle.Bold);
            this.scoreText.Location = new System.Drawing.Point(127, 500);
            this.scoreText.Name = "scoreText";
            this.scoreText.Size = new System.Drawing.Size(154, 41);
            this.scoreText.TabIndex = 4;
            this.scoreText.Text = "Score: 0";
            // 
            // gameTimer
            // 
            this.gameTimer.Enabled = true;
            this.gameTimer.Interval = 20;
            this.gameTimer.Tick += new System.EventHandler(this.gameTimerEvent);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.Color.Aqua;
            this.ClientSize = new System.Drawing.Size(597, 541);
            this.Controls.Add(this.scoreText);
            this.Controls.Add(this.HappyBirrd);
            this.Controls.Add(this.ground);
            this.Controls.Add(this.pipeBotton);
            this.Controls.Add(this.pipeTop);
            this.Name = "Form1";
            this.Text = "Happy Bird Game-Moo ICT ";
            this.Load += new System.EventHandler(this.Form1_Load);
            this.KeyDown += new System.Windows.Forms.KeyEventHandler(this.gamekeyisdown);
            this.KeyUp += new System.Windows.Forms.KeyEventHandler(this.gamekeyisup);
            ((System.ComponentModel.ISupportInitialize)(this.pipeTop)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.HappyBirrd)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.pipeBotton)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ground)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.PictureBox pipeTop;
        private System.Windows.Forms.PictureBox HappyBirrd;
        private System.Windows.Forms.PictureBox pipeBotton;
        private System.Windows.Forms.PictureBox ground;
        private System.Windows.Forms.Label scoreText;
        private System.Windows.Forms.Timer gameTimer;
    }
}

namespace Lokaverkefni
{
    partial class SpaceBar
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
            this.lblTimer = new System.Windows.Forms.Label();
            this.lblMain_Timer = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.lblclicks = new System.Windows.Forms.Label();
            this.timer1 = new System.Windows.Forms.Timer(this.components);
            this.pic1 = new System.Windows.Forms.PictureBox();
            ((System.ComponentModel.ISupportInitialize)(this.pic1)).BeginInit();
            this.SuspendLayout();
            // 
            // lblTimer
            // 
            this.lblTimer.AutoSize = true;
            this.lblTimer.Location = new System.Drawing.Point(12, 9);
            this.lblTimer.Name = "lblTimer";
            this.lblTimer.Size = new System.Drawing.Size(0, 13);
            this.lblTimer.TabIndex = 0;
            // 
            // lblMain_Timer
            // 
            this.lblMain_Timer.Font = new System.Drawing.Font("Modern No. 20", 8.249999F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblMain_Timer.Location = new System.Drawing.Point(12, 9);
            this.lblMain_Timer.Name = "lblMain_Timer";
            this.lblMain_Timer.Size = new System.Drawing.Size(124, 30);
            this.lblMain_Timer.TabIndex = 1;
            this.lblMain_Timer.Text = "Time Left: 10";
            this.lblMain_Timer.Click += new System.EventHandler(this.lblMain_Timer_Click);
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(356, 9);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(41, 13);
            this.label1.TabIndex = 2;
            this.label1.Text = ": Clicks";
            // 
            // lblclicks
            // 
            this.lblclicks.AutoSize = true;
            this.lblclicks.Location = new System.Drawing.Point(350, 9);
            this.lblclicks.Name = "lblclicks";
            this.lblclicks.Size = new System.Drawing.Size(0, 13);
            this.lblclicks.TabIndex = 3;
            // 
            // timer1
            // 
            this.timer1.Tick += new System.EventHandler(this.timer1_Tick);
            // 
            // pic1
            // 
            this.pic1.Image = global::Lokaverkefni.Properties.Resources.hand;
            this.pic1.InitialImage = global::Lokaverkefni.Properties.Resources.hand;
            this.pic1.Location = new System.Drawing.Point(32, 62);
            this.pic1.Name = "pic1";
            this.pic1.Size = new System.Drawing.Size(334, 339);
            this.pic1.SizeMode = System.Windows.Forms.PictureBoxSizeMode.StretchImage;
            this.pic1.TabIndex = 4;
            this.pic1.TabStop = false;
            // 
            // SpaceBar
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(409, 448);
            this.Controls.Add(this.pic1);
            this.Controls.Add(this.lblclicks);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.lblMain_Timer);
            this.Controls.Add(this.lblTimer);
            this.Name = "SpaceBar";
            this.Text = "SpaceBar";
            this.Load += new System.EventHandler(this.SpaceBar_Load);
            this.KeyDown += new System.Windows.Forms.KeyEventHandler(this.SpaceBar_KeyDown);
            ((System.ComponentModel.ISupportInitialize)(this.pic1)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label lblTimer;
        private System.Windows.Forms.Label lblMain_Timer;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label lblclicks;
        private System.Windows.Forms.Timer timer1;
        private System.Windows.Forms.PictureBox pic1;
    }
}
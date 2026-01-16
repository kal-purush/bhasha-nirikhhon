namespace Lokaverkefni
{
    partial class Village
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
            this.timer1 = new System.Windows.Forms.Timer(this.components);
            this.btnwood = new System.Windows.Forms.Button();
            this.btnsteel = new System.Windows.Forms.Button();
            this.btnmeat = new System.Windows.Forms.Button();
            this.btncoal = new System.Windows.Forms.Button();
            this.prbwood = new System.Windows.Forms.ProgressBar();
            this.prbsteel = new System.Windows.Forms.ProgressBar();
            this.prbmeat = new System.Windows.Forms.ProgressBar();
            this.prbcoal = new System.Windows.Forms.ProgressBar();
            this.lblcoins = new System.Windows.Forms.Label();
            this.piccoin1 = new System.Windows.Forms.PictureBox();
            this.lbllevel = new System.Windows.Forms.Label();
            ((System.ComponentModel.ISupportInitialize)(this.piccoin1)).BeginInit();
            this.SuspendLayout();
            // 
            // timer1
            // 
            this.timer1.Tick += new System.EventHandler(this.timer1_Tick);
            // 
            // btnwood
            // 
            this.btnwood.BackColor = System.Drawing.Color.Lime;
            this.btnwood.Location = new System.Drawing.Point(253, 438);
            this.btnwood.Name = "btnwood";
            this.btnwood.Size = new System.Drawing.Size(75, 48);
            this.btnwood.TabIndex = 0;
            this.btnwood.Text = "Wood";
            this.btnwood.UseVisualStyleBackColor = false;
            this.btnwood.Click += new System.EventHandler(this.btnwood_Click);
            // 
            // btnsteel
            // 
            this.btnsteel.BackColor = System.Drawing.Color.Lime;
            this.btnsteel.Location = new System.Drawing.Point(172, 438);
            this.btnsteel.Name = "btnsteel";
            this.btnsteel.Size = new System.Drawing.Size(75, 48);
            this.btnsteel.TabIndex = 1;
            this.btnsteel.Text = "Steel";
            this.btnsteel.UseVisualStyleBackColor = false;
            this.btnsteel.Click += new System.EventHandler(this.btnsteel_Click);
            // 
            // btnmeat
            // 
            this.btnmeat.BackColor = System.Drawing.Color.Lime;
            this.btnmeat.Location = new System.Drawing.Point(10, 438);
            this.btnmeat.Name = "btnmeat";
            this.btnmeat.Size = new System.Drawing.Size(75, 48);
            this.btnmeat.TabIndex = 2;
            this.btnmeat.Text = "Meat";
            this.btnmeat.UseVisualStyleBackColor = false;
            this.btnmeat.Click += new System.EventHandler(this.btnmeat_Click);
            // 
            // btncoal
            // 
            this.btncoal.BackColor = System.Drawing.Color.Lime;
            this.btncoal.Location = new System.Drawing.Point(91, 438);
            this.btncoal.Name = "btncoal";
            this.btncoal.Size = new System.Drawing.Size(75, 48);
            this.btncoal.TabIndex = 3;
            this.btncoal.Text = "Coal";
            this.btncoal.UseVisualStyleBackColor = false;
            this.btncoal.Click += new System.EventHandler(this.btncoal_Click);
            // 
            // prbwood
            // 
            this.prbwood.ForeColor = System.Drawing.SystemColors.ControlLight;
            this.prbwood.Location = new System.Drawing.Point(253, 404);
            this.prbwood.Name = "prbwood";
            this.prbwood.Size = new System.Drawing.Size(75, 18);
            this.prbwood.Step = 1;
            this.prbwood.TabIndex = 4;
            // 
            // prbsteel
            // 
            this.prbsteel.Location = new System.Drawing.Point(172, 404);
            this.prbsteel.Name = "prbsteel";
            this.prbsteel.Size = new System.Drawing.Size(75, 18);
            this.prbsteel.Step = 100;
            this.prbsteel.TabIndex = 5;
            // 
            // prbmeat
            // 
            this.prbmeat.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(255)))), ((int)(((byte)(224)))), ((int)(((byte)(192)))));
            this.prbmeat.Location = new System.Drawing.Point(10, 404);
            this.prbmeat.Name = "prbmeat";
            this.prbmeat.Size = new System.Drawing.Size(75, 18);
            this.prbmeat.Step = 100;
            this.prbmeat.Style = System.Windows.Forms.ProgressBarStyle.Continuous;
            this.prbmeat.TabIndex = 6;
            // 
            // prbcoal
            // 
            this.prbcoal.Location = new System.Drawing.Point(91, 404);
            this.prbcoal.Name = "prbcoal";
            this.prbcoal.Size = new System.Drawing.Size(75, 18);
            this.prbcoal.Step = 100;
            this.prbcoal.TabIndex = 7;
            // 
            // lblcoins
            // 
            this.lblcoins.BackColor = System.Drawing.Color.Transparent;
            this.lblcoins.Font = new System.Drawing.Font("Monotype Corsiva", 20.25F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblcoins.Location = new System.Drawing.Point(817, 437);
            this.lblcoins.Name = "lblcoins";
            this.lblcoins.Size = new System.Drawing.Size(64, 49);
            this.lblcoins.TabIndex = 8;
            this.lblcoins.Text = "C";
            // 
            // piccoin1
            // 
            this.piccoin1.BackColor = System.Drawing.Color.Transparent;
            this.piccoin1.Enabled = false;
            this.piccoin1.Image = global::Lokaverkefni.Properties.Resources.Coin;
            this.piccoin1.Location = new System.Drawing.Point(746, 426);
            this.piccoin1.Name = "piccoin1";
            this.piccoin1.Size = new System.Drawing.Size(65, 64);
            this.piccoin1.SizeMode = System.Windows.Forms.PictureBoxSizeMode.StretchImage;
            this.piccoin1.TabIndex = 9;
            this.piccoin1.TabStop = false;
            // 
            // lbllevel
            // 
            this.lbllevel.BackColor = System.Drawing.Color.Transparent;
            this.lbllevel.Font = new System.Drawing.Font("Monotype Corsiva", 20.25F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lbllevel.Location = new System.Drawing.Point(817, 9);
            this.lbllevel.Name = "lbllevel";
            this.lbllevel.Size = new System.Drawing.Size(64, 49);
            this.lbllevel.TabIndex = 11;
            this.lbllevel.Text = "L";
            // 
            // Village
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackgroundImage = global::Lokaverkefni.Properties.Resources.Backround1;
            this.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.ClientSize = new System.Drawing.Size(904, 494);
            this.Controls.Add(this.lbllevel);
            this.Controls.Add(this.piccoin1);
            this.Controls.Add(this.lblcoins);
            this.Controls.Add(this.prbcoal);
            this.Controls.Add(this.prbmeat);
            this.Controls.Add(this.prbsteel);
            this.Controls.Add(this.prbwood);
            this.Controls.Add(this.btncoal);
            this.Controls.Add(this.btnmeat);
            this.Controls.Add(this.btnsteel);
            this.Controls.Add(this.btnwood);
            this.DoubleBuffered = true;
            this.Name = "Village";
            this.Text = "Village";
            this.Load += new System.EventHandler(this.Village_Load);
            ((System.ComponentModel.ISupportInitialize)(this.piccoin1)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Timer timer1;
        private System.Windows.Forms.Button btnwood;
        private System.Windows.Forms.Button btnsteel;
        private System.Windows.Forms.Button btnmeat;
        private System.Windows.Forms.Button btncoal;
        private System.Windows.Forms.ProgressBar prbwood;
        private System.Windows.Forms.ProgressBar prbsteel;
        private System.Windows.Forms.ProgressBar prbmeat;
        private System.Windows.Forms.ProgressBar prbcoal;
        private System.Windows.Forms.Label lblcoins;
        private System.Windows.Forms.PictureBox piccoin1;
        private System.Windows.Forms.Label lbllevel;
    }
}
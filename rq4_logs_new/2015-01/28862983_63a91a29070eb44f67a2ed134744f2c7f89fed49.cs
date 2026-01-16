namespace WindowsFormsApplication1
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(Form1));
            this.button1 = new System.Windows.Forms.Button();
            this.hallBox = new System.Windows.Forms.TextBox();
            this.statusBox = new System.Windows.Forms.TextBox();
            this.label1 = new System.Windows.Forms.Label();
            this.ipBox = new System.Windows.Forms.TextBox();
            this.label2 = new System.Windows.Forms.Label();
            this.ironBu = new System.Windows.Forms.RadioButton();
            this.fortiBu = new System.Windows.Forms.RadioButton();
            this.write = new System.Windows.Forms.Button();
            this.dhcpBu = new System.Windows.Forms.RadioButton();
            this.button3 = new System.Windows.Forms.Button();
            this.button4 = new System.Windows.Forms.Button();
            this.button5 = new System.Windows.Forms.Button();
            this.oner = new System.Windows.Forms.Button();
            this.button6 = new System.Windows.Forms.Button();
            this.offer = new System.Windows.Forms.Button();
            this.adapBox = new System.Windows.Forms.TextBox();
            this.label3 = new System.Windows.Forms.Label();
            this.credits = new System.Windows.Forms.Label();
            this.pictureBox1 = new System.Windows.Forms.PictureBox();
            this.pictureBox2 = new System.Windows.Forms.PictureBox();
            this.pictureBox3 = new System.Windows.Forms.PictureBox();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox1)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox2)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox3)).BeginInit();
            this.SuspendLayout();
            // 
            // button1
            // 
            this.button1.Location = new System.Drawing.Point(72, 181);
            this.button1.Name = "button1";
            this.button1.Size = new System.Drawing.Size(212, 23);
            this.button1.TabIndex = 0;
            this.button1.Text = "Execute";
            this.button1.UseVisualStyleBackColor = true;
            this.button1.Click += new System.EventHandler(this.button1_Click);
            // 
            // hallBox
            // 
            this.hallBox.Location = new System.Drawing.Point(46, 12);
            this.hallBox.Name = "hallBox";
            this.hallBox.Size = new System.Drawing.Size(31, 20);
            this.hallBox.TabIndex = 1;
            this.hallBox.TextChanged += new System.EventHandler(this.textBox1_TextChanged);
            // 
            // statusBox
            // 
            this.statusBox.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.statusBox.Font = new System.Drawing.Font("Buxton Sketch", 15.75F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.statusBox.Location = new System.Drawing.Point(16, 140);
            this.statusBox.Name = "statusBox";
            this.statusBox.ReadOnly = true;
            this.statusBox.Size = new System.Drawing.Size(317, 26);
            this.statusBox.TabIndex = 2;
            this.statusBox.TabStop = false;
            this.statusBox.TextAlign = System.Windows.Forms.HorizontalAlignment.Center;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.BackColor = System.Drawing.SystemColors.ActiveBorder;
            this.label1.Location = new System.Drawing.Point(12, 15);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(28, 13);
            this.label1.TabIndex = 3;
            this.label1.Text = "Hall:";
            this.label1.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
            this.label1.Click += new System.EventHandler(this.label1_Click);
            // 
            // ipBox
            // 
            this.ipBox.Location = new System.Drawing.Point(281, 12);
            this.ipBox.Name = "ipBox";
            this.ipBox.Size = new System.Drawing.Size(51, 20);
            this.ipBox.TabIndex = 4;
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.BackColor = System.Drawing.SystemColors.ActiveBorder;
            this.label2.Location = new System.Drawing.Point(255, 15);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(20, 13);
            this.label2.TabIndex = 5;
            this.label2.Text = "IP:";
            this.label2.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
            this.label2.Click += new System.EventHandler(this.label2_Click);
            // 
            // ironBu
            // 
            this.ironBu.AutoSize = true;
            this.ironBu.BackColor = System.Drawing.SystemColors.ActiveBorder;
            this.ironBu.Location = new System.Drawing.Point(16, 60);
            this.ironBu.Name = "ironBu";
            this.ironBu.Size = new System.Drawing.Size(62, 17);
            this.ironBu.TabIndex = 6;
            this.ironBu.Text = "IronPort";
            this.ironBu.UseVisualStyleBackColor = false;
            // 
            // fortiBu
            // 
            this.fortiBu.AutoSize = true;
            this.fortiBu.BackColor = System.Drawing.SystemColors.ActiveBorder;
            this.fortiBu.Location = new System.Drawing.Point(270, 60);
            this.fortiBu.Name = "fortiBu";
            this.fortiBu.Size = new System.Drawing.Size(62, 17);
            this.fortiBu.TabIndex = 7;
            this.fortiBu.Text = "FortiNet";
            this.fortiBu.UseVisualStyleBackColor = false;
            // 
            // write
            // 
            this.write.Location = new System.Drawing.Point(72, 229);
            this.write.Name = "write";
            this.write.Size = new System.Drawing.Size(212, 23);
            this.write.TabIndex = 8;
            this.write.Text = "Write these settings to preset:  (select)";
            this.write.UseVisualStyleBackColor = true;
            this.write.Click += new System.EventHandler(this.write_Click_1);
            // 
            // dhcpBu
            // 
            this.dhcpBu.AutoSize = true;
            this.dhcpBu.BackColor = System.Drawing.SystemColors.ActiveBorder;
            this.dhcpBu.Location = new System.Drawing.Point(145, 60);
            this.dhcpBu.Name = "dhcpBu";
            this.dhcpBu.Size = new System.Drawing.Size(55, 17);
            this.dhcpBu.TabIndex = 12;
            this.dhcpBu.TabStop = true;
            this.dhcpBu.Text = "DHCP";
            this.dhcpBu.UseVisualStyleBackColor = false;
            this.dhcpBu.CheckedChanged += new System.EventHandler(this.radioButton4_CheckedChanged);
            // 
            // button3
            // 
            this.button3.Location = new System.Drawing.Point(16, 98);
            this.button3.Name = "button3";
            this.button3.Size = new System.Drawing.Size(75, 23);
            this.button3.TabIndex = 13;
            this.button3.Text = "Preset 1";
            this.button3.UseVisualStyleBackColor = true;
            this.button3.Click += new System.EventHandler(this.button3_Click);
            // 
            // button4
            // 
            this.button4.Location = new System.Drawing.Point(136, 98);
            this.button4.Name = "button4";
            this.button4.Size = new System.Drawing.Size(75, 23);
            this.button4.TabIndex = 14;
            this.button4.Text = "Preset 2";
            this.button4.UseVisualStyleBackColor = true;
            this.button4.Click += new System.EventHandler(this.button4_Click);
            // 
            // button5
            // 
            this.button5.Location = new System.Drawing.Point(258, 98);
            this.button5.Name = "button5";
            this.button5.Size = new System.Drawing.Size(75, 23);
            this.button5.TabIndex = 15;
            this.button5.Text = "Preset 3";
            this.button5.UseVisualStyleBackColor = true;
            this.button5.Click += new System.EventHandler(this.button5_Click_1);
            // 
            // oner
            // 
            this.oner.Location = new System.Drawing.Point(12, 273);
            this.oner.Name = "oner";
            this.oner.Size = new System.Drawing.Size(75, 23);
            this.oner.TabIndex = 16;
            this.oner.Text = "Hotspot On";
            this.oner.UseVisualStyleBackColor = true;
            this.oner.Click += new System.EventHandler(this.oner_Click);
            // 
            // button6
            // 
            this.button6.Location = new System.Drawing.Point(125, 273);
            this.button6.Name = "button6";
            this.button6.Size = new System.Drawing.Size(105, 23);
            this.button6.TabIndex = 17;
            this.button6.Text = "Configure hotspot";
            this.button6.UseVisualStyleBackColor = true;
            this.button6.Click += new System.EventHandler(this.button6_Click);
            // 
            // offer
            // 
            this.offer.Location = new System.Drawing.Point(258, 273);
            this.offer.Name = "offer";
            this.offer.Size = new System.Drawing.Size(75, 23);
            this.offer.TabIndex = 18;
            this.offer.Text = "Hotspot Off";
            this.offer.UseVisualStyleBackColor = true;
            this.offer.Click += new System.EventHandler(this.offer_Click);
            // 
            // adapBox
            // 
            this.adapBox.Location = new System.Drawing.Point(145, 12);
            this.adapBox.Name = "adapBox";
            this.adapBox.Size = new System.Drawing.Size(93, 20);
            this.adapBox.TabIndex = 19;
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.BackColor = System.Drawing.SystemColors.ActiveBorder;
            this.label3.Location = new System.Drawing.Point(92, 15);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(47, 13);
            this.label3.TabIndex = 20;
            this.label3.Text = "Adapter:";
            this.label3.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
            // 
            // credits
            // 
            this.credits.AutoSize = true;
            this.credits.BackColor = System.Drawing.SystemColors.ActiveBorder;
            this.credits.Cursor = System.Windows.Forms.Cursors.Hand;
            this.credits.Font = new System.Drawing.Font("Mistral", 18F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.credits.Location = new System.Drawing.Point(186, 310);
            this.credits.Name = "credits";
            this.credits.Size = new System.Drawing.Size(161, 29);
            this.credits.TabIndex = 21;
            this.credits.Text = "IITkNet by Saksham";
            this.credits.Click += new System.EventHandler(this.credits_Click);
            // 
            // pictureBox1
            // 
            this.pictureBox1.Location = new System.Drawing.Point(12, 303);
            this.pictureBox1.Name = "pictureBox1";
            this.pictureBox1.Size = new System.Drawing.Size(37, 36);
            this.pictureBox1.TabIndex = 22;
            this.pictureBox1.TabStop = false;
            this.pictureBox1.Click += new System.EventHandler(this.pictureBox1_Click);
            // 
            // pictureBox2
            // 
            this.pictureBox2.Image = ((System.Drawing.Image)(resources.GetObject("pictureBox2.Image")));
            this.pictureBox2.Location = new System.Drawing.Point(-6, -2);
            this.pictureBox2.Name = "pictureBox2";
            this.pictureBox2.Size = new System.Drawing.Size(361, 352);
            this.pictureBox2.TabIndex = 23;
            this.pictureBox2.TabStop = false;
            // 
            // pictureBox3
            // 
            this.pictureBox3.Location = new System.Drawing.Point(55, 303);
            this.pictureBox3.Name = "pictureBox3";
            this.pictureBox3.Size = new System.Drawing.Size(37, 36);
            this.pictureBox3.TabIndex = 24;
            this.pictureBox3.TabStop = false;
            this.pictureBox3.Click += new System.EventHandler(this.pictureBox3_Click);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.SystemColors.GradientActiveCaption;
            this.ClientSize = new System.Drawing.Size(345, 342);
            this.Controls.Add(this.pictureBox3);
            this.Controls.Add(this.pictureBox1);
            this.Controls.Add(this.credits);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.adapBox);
            this.Controls.Add(this.offer);
            this.Controls.Add(this.button6);
            this.Controls.Add(this.oner);
            this.Controls.Add(this.button5);
            this.Controls.Add(this.button4);
            this.Controls.Add(this.button3);
            this.Controls.Add(this.dhcpBu);
            this.Controls.Add(this.write);
            this.Controls.Add(this.fortiBu);
            this.Controls.Add(this.ironBu);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.ipBox);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.statusBox);
            this.Controls.Add(this.hallBox);
            this.Controls.Add(this.button1);
            this.Controls.Add(this.pictureBox2);
            this.KeyPreview = true;
            this.Name = "Form1";
            this.Text = "IITkNet";
            this.Load += new System.EventHandler(this.Form1_Load);
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox1)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox2)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox3)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button button1;
        private System.Windows.Forms.TextBox hallBox;
        private System.Windows.Forms.TextBox statusBox;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.TextBox ipBox;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.RadioButton ironBu;
        private System.Windows.Forms.RadioButton fortiBu;
        private System.Windows.Forms.Button write;
        private System.Windows.Forms.RadioButton dhcpBu;
        private System.Windows.Forms.Button button3;
        private System.Windows.Forms.Button button4;
        private System.Windows.Forms.Button button5;
        private System.Windows.Forms.Button oner;
        private System.Windows.Forms.Button button6;
        private System.Windows.Forms.Button offer;
        private System.Windows.Forms.TextBox adapBox;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.Label credits;
        private System.Windows.Forms.PictureBox pictureBox1;
        private System.Windows.Forms.PictureBox pictureBox2;
        private System.Windows.Forms.PictureBox pictureBox3;
    }
}

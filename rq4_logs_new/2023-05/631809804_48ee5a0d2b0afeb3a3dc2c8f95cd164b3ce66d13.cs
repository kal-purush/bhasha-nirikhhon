namespace Borromeo_Filomeno_FinalProject
{
    partial class Math_Zilla
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
            this.txt_Answer = new System.Windows.Forms.TextBox();
            this.pictureBox1 = new System.Windows.Forms.PictureBox();
            this.label1 = new System.Windows.Forms.Label();
            this.panel_Design = new System.Windows.Forms.Panel();
            this.label2 = new System.Windows.Forms.Label();
            this.button1 = new System.Windows.Forms.Button();
            this.panel_Name = new System.Windows.Forms.Panel();
            this.lbl_Player2Name = new System.Windows.Forms.Label();
            this.lbl_Player1Name = new System.Windows.Forms.Label();
            this.txt_Player2Name = new System.Windows.Forms.TextBox();
            this.txt_Player1Name = new System.Windows.Forms.TextBox();
            this.panel_Problem = new System.Windows.Forms.Panel();
            this.label6 = new System.Windows.Forms.Label();
            this.lbl_Display_Numeric_Problem = new System.Windows.Forms.Label();
            this.pictureBox2 = new System.Windows.Forms.PictureBox();
            this.panel_Answering = new System.Windows.Forms.Panel();
            this.btnSubmit = new System.Windows.Forms.Button();
            this.btn_Clue = new System.Windows.Forms.Button();
            this.label3 = new System.Windows.Forms.Label();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox1)).BeginInit();
            this.panel_Name.SuspendLayout();
            this.panel_Problem.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox2)).BeginInit();
            this.panel_Answering.SuspendLayout();
            this.SuspendLayout();
            // 
            // txt_Answer
            // 
            this.txt_Answer.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(216)))), ((int)(((byte)(253)))));
            this.txt_Answer.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.txt_Answer.Location = new System.Drawing.Point(312, 39);
            this.txt_Answer.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.txt_Answer.Name = "txt_Answer";
            this.txt_Answer.Size = new System.Drawing.Size(150, 22);
            this.txt_Answer.TabIndex = 0;
            this.txt_Answer.Text = "tenp";
            this.txt_Answer.TextAlign = System.Windows.Forms.HorizontalAlignment.Center;
            this.txt_Answer.TextChanged += new System.EventHandler(this.textBox1_TextChanged);
            // 
            // pictureBox1
            // 
            this.pictureBox1.BackColor = System.Drawing.Color.Transparent;
            this.pictureBox1.Image = global::Borromeo_Filomeno_FinalProject.Properties.Resources.MathZilla_title;
            this.pictureBox1.Location = new System.Drawing.Point(290, 31);
            this.pictureBox1.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.pictureBox1.Name = "pictureBox1";
            this.pictureBox1.Size = new System.Drawing.Size(267, 48);
            this.pictureBox1.SizeMode = System.Windows.Forms.PictureBoxSizeMode.AutoSize;
            this.pictureBox1.TabIndex = 1;
            this.pictureBox1.TabStop = false;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.BackColor = System.Drawing.Color.Transparent;
            this.label1.Location = new System.Drawing.Point(33, 18);
            this.label1.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(61, 21);
            this.label1.TabIndex = 2;
            this.label1.Text = "Name :";
            this.label1.Click += new System.EventHandler(this.label1_Click);
            // 
            // panel_Design
            // 
            this.panel_Design.BackColor = System.Drawing.Color.White;
            this.panel_Design.ForeColor = System.Drawing.SystemColors.ActiveCaption;
            this.panel_Design.Location = new System.Drawing.Point(312, 62);
            this.panel_Design.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.panel_Design.Name = "panel_Design";
            this.panel_Design.Size = new System.Drawing.Size(150, 3);
            this.panel_Design.TabIndex = 3;
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.BackColor = System.Drawing.Color.Transparent;
            this.label2.Location = new System.Drawing.Point(578, 19);
            this.label2.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(61, 21);
            this.label2.TabIndex = 2;
            this.label2.Text = "Name :";
            // 
            // button1
            // 
            this.button1.BackColor = System.Drawing.Color.Transparent;
            this.button1.FlatAppearance.BorderSize = 0;
            this.button1.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.button1.Location = new System.Drawing.Point(12, 598);
            this.button1.Name = "button1";
            this.button1.Size = new System.Drawing.Size(145, 37);
            this.button1.TabIndex = 4;
            this.button1.Text = "Change Name";
            this.button1.UseVisualStyleBackColor = false;
            // 
            // panel_Name
            // 
            this.panel_Name.BackColor = System.Drawing.Color.Transparent;
            this.panel_Name.Controls.Add(this.label2);
            this.panel_Name.Controls.Add(this.lbl_Player2Name);
            this.panel_Name.Controls.Add(this.lbl_Player1Name);
            this.panel_Name.Controls.Add(this.label1);
            this.panel_Name.Controls.Add(this.txt_Player2Name);
            this.panel_Name.Controls.Add(this.txt_Player1Name);
            this.panel_Name.Location = new System.Drawing.Point(23, 81);
            this.panel_Name.Name = "panel_Name";
            this.panel_Name.Size = new System.Drawing.Size(801, 103);
            this.panel_Name.TabIndex = 5;
            // 
            // lbl_Player2Name
            // 
            this.lbl_Player2Name.AutoSize = true;
            this.lbl_Player2Name.BackColor = System.Drawing.Color.Transparent;
            this.lbl_Player2Name.Location = new System.Drawing.Point(567, 57);
            this.lbl_Player2Name.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lbl_Player2Name.Name = "lbl_Player2Name";
            this.lbl_Player2Name.Size = new System.Drawing.Size(129, 21);
            this.lbl_Player2Name.TabIndex = 2;
            this.lbl_Player2Name.Text = "Display p2 name";
            // 
            // lbl_Player1Name
            // 
            this.lbl_Player1Name.AutoSize = true;
            this.lbl_Player1Name.BackColor = System.Drawing.Color.Transparent;
            this.lbl_Player1Name.Location = new System.Drawing.Point(88, 57);
            this.lbl_Player1Name.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lbl_Player1Name.Name = "lbl_Player1Name";
            this.lbl_Player1Name.Size = new System.Drawing.Size(126, 21);
            this.lbl_Player1Name.TabIndex = 2;
            this.lbl_Player1Name.Text = "Display p1 name";
            // 
            // txt_Player2Name
            // 
            this.txt_Player2Name.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(216)))), ((int)(((byte)(253)))));
            this.txt_Player2Name.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.txt_Player2Name.Location = new System.Drawing.Point(641, 21);
            this.txt_Player2Name.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.txt_Player2Name.Name = "txt_Player2Name";
            this.txt_Player2Name.Size = new System.Drawing.Size(150, 22);
            this.txt_Player2Name.TabIndex = 0;
            this.txt_Player2Name.Text = "tenp";
            this.txt_Player2Name.TextAlign = System.Windows.Forms.HorizontalAlignment.Center;
            this.txt_Player2Name.TextChanged += new System.EventHandler(this.textBox1_TextChanged);
            this.txt_Player2Name.KeyPress += new System.Windows.Forms.KeyPressEventHandler(this.txt_Player2Name_KeyPress);
            // 
            // txt_Player1Name
            // 
            this.txt_Player1Name.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(216)))), ((int)(((byte)(253)))));
            this.txt_Player1Name.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.txt_Player1Name.Location = new System.Drawing.Point(92, 20);
            this.txt_Player1Name.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.txt_Player1Name.Name = "txt_Player1Name";
            this.txt_Player1Name.Size = new System.Drawing.Size(150, 22);
            this.txt_Player1Name.TabIndex = 0;
            this.txt_Player1Name.Text = "tenp";
            this.txt_Player1Name.TextAlign = System.Windows.Forms.HorizontalAlignment.Center;
            this.txt_Player1Name.KeyPress += new System.Windows.Forms.KeyPressEventHandler(this.txt_Player1Name_KeyPress);
            // 
            // panel_Problem
            // 
            this.panel_Problem.BackColor = System.Drawing.Color.Transparent;
            this.panel_Problem.Controls.Add(this.label6);
            this.panel_Problem.Controls.Add(this.lbl_Display_Numeric_Problem);
            this.panel_Problem.Controls.Add(this.pictureBox2);
            this.panel_Problem.Location = new System.Drawing.Point(23, 225);
            this.panel_Problem.Name = "panel_Problem";
            this.panel_Problem.Size = new System.Drawing.Size(801, 131);
            this.panel_Problem.TabIndex = 6;
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Location = new System.Drawing.Point(221, 85);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(323, 21);
            this.label6.TabIndex = 1;
            this.label6.Text = "mu create og 5 ka pictures og 4 ka pictures\r\n";
            // 
            // lbl_Display_Numeric_Problem
            // 
            this.lbl_Display_Numeric_Problem.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.lbl_Display_Numeric_Problem.AutoSize = true;
            this.lbl_Display_Numeric_Problem.Location = new System.Drawing.Point(361, 39);
            this.lbl_Display_Numeric_Problem.Name = "lbl_Display_Numeric_Problem";
            this.lbl_Display_Numeric_Problem.Size = new System.Drawing.Size(87, 21);
            this.lbl_Display_Numeric_Problem.TabIndex = 1;
            this.lbl_Display_Numeric_Problem.Text = "temp 5 x 4";
            this.lbl_Display_Numeric_Problem.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // pictureBox2
            // 
            this.pictureBox2.Location = new System.Drawing.Point(132, 16);
            this.pictureBox2.Name = "pictureBox2";
            this.pictureBox2.Size = new System.Drawing.Size(45, 35);
            this.pictureBox2.TabIndex = 0;
            this.pictureBox2.TabStop = false;
            // 
            // panel_Answering
            // 
            this.panel_Answering.BackColor = System.Drawing.Color.Transparent;
            this.panel_Answering.Controls.Add(this.btn_Clue);
            this.panel_Answering.Controls.Add(this.label3);
            this.panel_Answering.Controls.Add(this.btnSubmit);
            this.panel_Answering.Controls.Add(this.txt_Answer);
            this.panel_Answering.Controls.Add(this.panel_Design);
            this.panel_Answering.Location = new System.Drawing.Point(32, 380);
            this.panel_Answering.Name = "panel_Answering";
            this.panel_Answering.Size = new System.Drawing.Size(774, 114);
            this.panel_Answering.TabIndex = 7;
            // 
            // btnSubmit
            // 
            this.btnSubmit.FlatAppearance.BorderSize = 0;
            this.btnSubmit.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btnSubmit.Location = new System.Drawing.Point(350, 74);
            this.btnSubmit.Name = "btnSubmit";
            this.btnSubmit.Size = new System.Drawing.Size(75, 27);
            this.btnSubmit.TabIndex = 4;
            this.btnSubmit.Text = "Submit";
            this.btnSubmit.UseVisualStyleBackColor = true;
            this.btnSubmit.Click += new System.EventHandler(this.btnSubmit_Click);
            // 
            // btn_Clue
            // 
            this.btn_Clue.FlatAppearance.BorderSize = 0;
            this.btn_Clue.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btn_Clue.Location = new System.Drawing.Point(603, 63);
            this.btn_Clue.Name = "btn_Clue";
            this.btn_Clue.Size = new System.Drawing.Size(75, 27);
            this.btn_Clue.TabIndex = 4;
            this.btn_Clue.Text = "Clue";
            this.btn_Clue.UseVisualStyleBackColor = true;
            this.btn_Clue.Click += new System.EventHandler(this.btn_Clue_Click);
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(596, 39);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(93, 21);
            this.label3.TabIndex = 1;
            this.label3.Text = "Need help?";
            // 
            // Math_Zilla
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(9F, 21F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackgroundImage = global::Borromeo_Filomeno_FinalProject.Properties.Resources.blue_gradient_bg;
            this.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.ClientSize = new System.Drawing.Size(847, 647);
            this.Controls.Add(this.panel_Problem);
            this.Controls.Add(this.button1);
            this.Controls.Add(this.pictureBox1);
            this.Controls.Add(this.panel_Name);
            this.Controls.Add(this.panel_Answering);
            this.Font = new System.Drawing.Font("Segoe UI Semibold", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.None;
            this.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.Name = "Math_Zilla";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "te";
            this.Load += new System.EventHandler(this.Math_Zilla_Load);
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox1)).EndInit();
            this.panel_Name.ResumeLayout(false);
            this.panel_Name.PerformLayout();
            this.panel_Problem.ResumeLayout(false);
            this.panel_Problem.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox2)).EndInit();
            this.panel_Answering.ResumeLayout(false);
            this.panel_Answering.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox txt_Answer;
        private System.Windows.Forms.PictureBox pictureBox1;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Panel panel_Design;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Button button1;
        private System.Windows.Forms.Panel panel_Name;
        private System.Windows.Forms.Label lbl_Player2Name;
        private System.Windows.Forms.Label lbl_Player1Name;
        private System.Windows.Forms.TextBox txt_Player2Name;
        private System.Windows.Forms.TextBox txt_Player1Name;
        private System.Windows.Forms.Panel panel_Problem;
        private System.Windows.Forms.PictureBox pictureBox2;
        private System.Windows.Forms.Panel panel_Answering;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.Label lbl_Display_Numeric_Problem;
        private System.Windows.Forms.Button btn_Clue;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.Button btnSubmit;
    }
}
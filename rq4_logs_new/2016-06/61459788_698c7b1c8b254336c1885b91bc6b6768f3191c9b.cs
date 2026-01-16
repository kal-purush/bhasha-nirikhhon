namespace CaptIt
{
    partial class ProgramInfo
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
            this.pFront = new System.Windows.Forms.Panel();
            this.lbInfo = new System.Windows.Forms.Label();
            this.pbIcon = new System.Windows.Forms.PictureBox();
            this.lbName = new System.Windows.Forms.Label();
            this.btnMinimize = new System.Windows.Forms.Button();
            this.btnClose = new System.Windows.Forms.Button();
            this.btnHouse = new System.Windows.Forms.Button();
            this.pFront.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.pbIcon)).BeginInit();
            this.SuspendLayout();
            // 
            // pFront
            // 
            this.pFront.BackColor = System.Drawing.Color.White;
            this.pFront.Controls.Add(this.lbInfo);
            this.pFront.Controls.Add(this.pbIcon);
            this.pFront.Controls.Add(this.lbName);
            this.pFront.Controls.Add(this.btnMinimize);
            this.pFront.Controls.Add(this.btnClose);
            this.pFront.Location = new System.Drawing.Point(1, 1);
            this.pFront.Margin = new System.Windows.Forms.Padding(0);
            this.pFront.Name = "pFront";
            this.pFront.Size = new System.Drawing.Size(398, 169);
            this.pFront.TabIndex = 3;
            this.pFront.MouseDown += new System.Windows.Forms.MouseEventHandler(this.pFront_MouseDown);
            this.pFront.MouseMove += new System.Windows.Forms.MouseEventHandler(this.pFront_MouseMove);
            // 
            // lbInfo
            // 
            this.lbInfo.Location = new System.Drawing.Point(113, 44);
            this.lbInfo.Name = "lbInfo";
            this.lbInfo.Size = new System.Drawing.Size(274, 96);
            this.lbInfo.TabIndex = 6;
            this.lbInfo.Text = "CaptIt (캡칫) v1.0.0\r\n\r\n공식 블로그 : http://studio-sonagi.tistory.com/\r\n\r\nCopyright(c)2" +
    "016\r\nStudio Sonagi All rights reserved.";
            // 
            // pbIcon
            // 
            this.pbIcon.Image = global::CaptIt.Resources1.CamNormal;
            this.pbIcon.Location = new System.Drawing.Point(11, 44);
            this.pbIcon.Name = "pbIcon";
            this.pbIcon.Size = new System.Drawing.Size(96, 96);
            this.pbIcon.SizeMode = System.Windows.Forms.PictureBoxSizeMode.AutoSize;
            this.pbIcon.TabIndex = 5;
            this.pbIcon.TabStop = false;
            // 
            // lbName
            // 
            this.lbName.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(51)))), ((int)(((byte)(173)))), ((int)(((byte)(255)))));
            this.lbName.Location = new System.Drawing.Point(3, 6);
            this.lbName.Name = "lbName";
            this.lbName.Size = new System.Drawing.Size(81, 25);
            this.lbName.TabIndex = 4;
            this.lbName.Text = "CaptIt - 정보";
            // 
            // btnMinimize
            // 
            this.btnMinimize.FlatAppearance.BorderSize = 0;
            this.btnMinimize.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btnMinimize.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(51)))), ((int)(((byte)(173)))), ((int)(((byte)(255)))));
            this.btnMinimize.Location = new System.Drawing.Point(347, 1);
            this.btnMinimize.Margin = new System.Windows.Forms.Padding(0);
            this.btnMinimize.Name = "btnMinimize";
            this.btnMinimize.Size = new System.Drawing.Size(25, 25);
            this.btnMinimize.TabIndex = 3;
            this.btnMinimize.TabStop = false;
            this.btnMinimize.Text = "_";
            this.btnMinimize.UseVisualStyleBackColor = true;
            this.btnMinimize.Click += new System.EventHandler(this.btnMinimize_Click);
            // 
            // btnClose
            // 
            this.btnClose.FlatAppearance.BorderSize = 0;
            this.btnClose.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btnClose.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(51)))), ((int)(((byte)(173)))), ((int)(((byte)(255)))));
            this.btnClose.Location = new System.Drawing.Point(372, 1);
            this.btnClose.Margin = new System.Windows.Forms.Padding(0);
            this.btnClose.Name = "btnClose";
            this.btnClose.Size = new System.Drawing.Size(25, 25);
            this.btnClose.TabIndex = 2;
            this.btnClose.TabStop = false;
            this.btnClose.Text = "x";
            this.btnClose.UseVisualStyleBackColor = true;
            this.btnClose.Click += new System.EventHandler(this.btnClose_Click);
            // 
            // btnHouse
            // 
            this.btnHouse.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(97)))), ((int)(((byte)(192)))), ((int)(((byte)(255)))));
            this.btnHouse.Cursor = System.Windows.Forms.Cursors.Default;
            this.btnHouse.FlatAppearance.BorderSize = 0;
            this.btnHouse.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btnHouse.Location = new System.Drawing.Point(378, 170);
            this.btnHouse.Margin = new System.Windows.Forms.Padding(0);
            this.btnHouse.Name = "btnHouse";
            this.btnHouse.Size = new System.Drawing.Size(20, 20);
            this.btnHouse.TabIndex = 5;
            this.btnHouse.TabStop = false;
            this.btnHouse.UseVisualStyleBackColor = false;
            this.btnHouse.Click += new System.EventHandler(this.btnHouse_Click);
            this.btnHouse.Paint += new System.Windows.Forms.PaintEventHandler(this.btnHouse_Paint);
            // 
            // ProgramInfo
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(7F, 15F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(97)))), ((int)(((byte)(192)))), ((int)(((byte)(255)))));
            this.ClientSize = new System.Drawing.Size(400, 190);
            this.Controls.Add(this.btnHouse);
            this.Controls.Add(this.pFront);
            this.Font = new System.Drawing.Font("맑은 고딕", 9F);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.None;
            this.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.Name = "ProgramInfo";
            this.Text = "ProgramInfo";
            this.pFront.ResumeLayout(false);
            this.pFront.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.pbIcon)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Panel pFront;
        private System.Windows.Forms.Label lbName;
        private System.Windows.Forms.Button btnMinimize;
        private System.Windows.Forms.Button btnClose;
        private System.Windows.Forms.Label lbInfo;
        private System.Windows.Forms.PictureBox pbIcon;
        private System.Windows.Forms.Button btnHouse;
    }
}
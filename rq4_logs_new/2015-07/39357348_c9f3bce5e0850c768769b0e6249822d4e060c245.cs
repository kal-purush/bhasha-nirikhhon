namespace DataCorrectionGui
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
            this.openFileName = new System.Windows.Forms.TextBox();
            this.openFile = new System.Windows.Forms.Button();
            this.saveAsButton = new System.Windows.Forms.Button();
            this.saveFileName = new System.Windows.Forms.TextBox();
            this.WavlengthRangeLabel = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.startText = new System.Windows.Forms.TextBox();
            this.endText = new System.Windows.Forms.TextBox();
            this.START = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // openFileName
            // 
            this.openFileName.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F);
            this.openFileName.Location = new System.Drawing.Point(136, 44);
            this.openFileName.Name = "openFileName";
            this.openFileName.Size = new System.Drawing.Size(244, 20);
            this.openFileName.TabIndex = 0;
            this.openFileName.Text = " ";
            this.openFileName.TextChanged += new System.EventHandler(this.openFileName_TextChanged);
            // 
            // openFile
            // 
            this.openFile.Location = new System.Drawing.Point(42, 40);
            this.openFile.Name = "openFile";
            this.openFile.Size = new System.Drawing.Size(75, 23);
            this.openFile.TabIndex = 1;
            this.openFile.Text = "Open";
            this.openFile.UseVisualStyleBackColor = true;
            this.openFile.Click += new System.EventHandler(this.button1_Click);
            // 
            // saveAsButton
            // 
            this.saveAsButton.Location = new System.Drawing.Point(42, 96);
            this.saveAsButton.Name = "saveAsButton";
            this.saveAsButton.Size = new System.Drawing.Size(75, 23);
            this.saveAsButton.TabIndex = 3;
            this.saveAsButton.Text = "Save As";
            this.saveAsButton.UseVisualStyleBackColor = true;
            this.saveAsButton.Click += new System.EventHandler(this.button2_Click);
            // 
            // saveFileName
            // 
            this.saveFileName.Location = new System.Drawing.Point(136, 100);
            this.saveFileName.Name = "saveFileName";
            this.saveFileName.Size = new System.Drawing.Size(244, 20);
            this.saveFileName.TabIndex = 2;
            this.saveFileName.TextChanged += new System.EventHandler(this.textBox2_TextChanged);
            // 
            // WavlengthRangeLabel
            // 
            this.WavlengthRangeLabel.AutoSize = true;
            this.WavlengthRangeLabel.BackColor = System.Drawing.SystemColors.ControlDark;
            this.WavlengthRangeLabel.BorderStyle = System.Windows.Forms.BorderStyle.Fixed3D;
            this.WavlengthRangeLabel.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.WavlengthRangeLabel.ForeColor = System.Drawing.Color.Blue;
            this.WavlengthRangeLabel.Location = new System.Drawing.Point(119, 159);
            this.WavlengthRangeLabel.Name = "WavlengthRangeLabel";
            this.WavlengthRangeLabel.Size = new System.Drawing.Size(163, 22);
            this.WavlengthRangeLabel.TabIndex = 4;
            this.WavlengthRangeLabel.Text = "Wavelength Range";
            this.WavlengthRangeLabel.Click += new System.EventHandler(this.label1_Click);
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label2.Location = new System.Drawing.Point(79, 213);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(35, 16);
            this.label2.TabIndex = 5;
            this.label2.Text = "Start";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label3.Location = new System.Drawing.Point(82, 245);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(32, 16);
            this.label3.TabIndex = 6;
            this.label3.Text = "End";
            // 
            // startText
            // 
            this.startText.Location = new System.Drawing.Point(136, 213);
            this.startText.Name = "startText";
            this.startText.Size = new System.Drawing.Size(63, 20);
            this.startText.TabIndex = 7;
            this.startText.TextChanged += new System.EventHandler(this.textBox3_TextChanged);
            // 
            // endText
            // 
            this.endText.Location = new System.Drawing.Point(136, 245);
            this.endText.Name = "endText";
            this.endText.Size = new System.Drawing.Size(63, 20);
            this.endText.TabIndex = 8;
            this.endText.TextChanged += new System.EventHandler(this.endText_TextChanged);
            // 
            // START
            // 
            this.START.BackColor = System.Drawing.Color.LightGreen;
            this.START.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.START.Font = new System.Drawing.Font("Microsoft Sans Serif", 15.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.START.ForeColor = System.Drawing.SystemColors.ActiveCaptionText;
            this.START.Location = new System.Drawing.Point(265, 200);
            this.START.Name = "START";
            this.START.RightToLeft = System.Windows.Forms.RightToLeft.Yes;
            this.START.Size = new System.Drawing.Size(103, 77);
            this.START.TabIndex = 9;
            this.START.Text = "START";
            this.START.UseVisualStyleBackColor = false;
            this.START.Click += new System.EventHandler(this.START_Click);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.AutoValidate = System.Windows.Forms.AutoValidate.EnablePreventFocusChange;
            this.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("$this.BackgroundImage")));
            this.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.ClientSize = new System.Drawing.Size(410, 327);
            this.Controls.Add(this.START);
            this.Controls.Add(this.endText);
            this.Controls.Add(this.startText);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.WavlengthRangeLabel);
            this.Controls.Add(this.saveAsButton);
            this.Controls.Add(this.saveFileName);
            this.Controls.Add(this.openFile);
            this.Controls.Add(this.openFileName);
            this.Name = "Form1";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Data Correction";
            this.Load += new System.EventHandler(this.Form1_Load);
            this.Move += new System.EventHandler(this.Form1_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox openFileName;
        private System.Windows.Forms.Button openFile;
        private System.Windows.Forms.Button saveAsButton;
        private System.Windows.Forms.TextBox saveFileName;
        private System.Windows.Forms.Label WavlengthRangeLabel;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.TextBox startText;
        private System.Windows.Forms.TextBox endText;
        private System.Windows.Forms.Button START;
    }
}

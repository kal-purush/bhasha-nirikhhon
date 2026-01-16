namespace LightsOut
{
    partial class AboutForm
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(AboutForm));
            this.aboutTitle = new System.Windows.Forms.Label();
            this.aboutName = new System.Windows.Forms.Label();
            this.aboutInstructions = new System.Windows.Forms.Label();
            this.pictureBox1 = new System.Windows.Forms.PictureBox();
            this.aboutButtonOK = new System.Windows.Forms.Button();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox1)).BeginInit();
            this.SuspendLayout();
            // 
            // aboutTitle
            // 
            this.aboutTitle.AutoSize = true;
            this.aboutTitle.Font = new System.Drawing.Font("Microsoft Sans Serif", 14.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.aboutTitle.ForeColor = System.Drawing.Color.Red;
            this.aboutTitle.Location = new System.Drawing.Point(104, 18);
            this.aboutTitle.Name = "aboutTitle";
            this.aboutTitle.Size = new System.Drawing.Size(110, 24);
            this.aboutTitle.TabIndex = 0;
            this.aboutTitle.Text = "Lights Out!";
            // 
            // aboutName
            // 
            this.aboutName.AutoSize = true;
            this.aboutName.Location = new System.Drawing.Point(105, 52);
            this.aboutName.Name = "aboutName";
            this.aboutName.Size = new System.Drawing.Size(89, 13);
            this.aboutName.TabIndex = 1;
            this.aboutName.Text = "By Dylan Watson";
            // 
            // aboutInstructions
            // 
            this.aboutInstructions.AutoSize = true;
            this.aboutInstructions.Location = new System.Drawing.Point(36, 81);
            this.aboutInstructions.Name = "aboutInstructions";
            this.aboutInstructions.Size = new System.Drawing.Size(221, 13);
            this.aboutInstructions.TabIndex = 2;
            this.aboutInstructions.Text = "Turn all the lights out (black) to win the game!";
            // 
            // pictureBox1
            // 
            this.pictureBox1.Image = ((System.Drawing.Image)(resources.GetObject("pictureBox1.Image")));
            this.pictureBox1.InitialImage = null;
            this.pictureBox1.Location = new System.Drawing.Point(57, 18);
            this.pictureBox1.Name = "pictureBox1";
            this.pictureBox1.Size = new System.Drawing.Size(41, 56);
            this.pictureBox1.TabIndex = 3;
            this.pictureBox1.TabStop = false;
            // 
            // aboutButtonOK
            // 
            this.aboutButtonOK.DialogResult = System.Windows.Forms.DialogResult.OK;
            this.aboutButtonOK.Location = new System.Drawing.Point(108, 129);
            this.aboutButtonOK.Name = "aboutButtonOK";
            this.aboutButtonOK.Size = new System.Drawing.Size(75, 23);
            this.aboutButtonOK.TabIndex = 4;
            this.aboutButtonOK.Text = "OK";
            this.aboutButtonOK.UseVisualStyleBackColor = true;
            // 
            // AboutForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(284, 177);
            this.Controls.Add(this.aboutButtonOK);
            this.Controls.Add(this.pictureBox1);
            this.Controls.Add(this.aboutInstructions);
            this.Controls.Add(this.aboutName);
            this.Controls.Add(this.aboutTitle);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedDialog;
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "AboutForm";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterParent;
            this.Text = "About";
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox1)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label aboutTitle;
        private System.Windows.Forms.Label aboutName;
        private System.Windows.Forms.Label aboutInstructions;
        private System.Windows.Forms.PictureBox pictureBox1;
        private System.Windows.Forms.Button aboutButtonOK;
    }
}
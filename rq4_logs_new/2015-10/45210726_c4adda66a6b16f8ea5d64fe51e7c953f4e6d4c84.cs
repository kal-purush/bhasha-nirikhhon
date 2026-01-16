namespace BryanGame
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
            this.showButton = new System.Windows.Forms.Button();
            this.fettPicture = new System.Windows.Forms.PictureBox();
            this.hideButton = new System.Windows.Forms.Button();
            this.fett2Picture = new System.Windows.Forms.PictureBox();
            ((System.ComponentModel.ISupportInitialize)(this.fettPicture)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.fett2Picture)).BeginInit();
            this.SuspendLayout();
            // 
            // showButton
            // 
            this.showButton.Location = new System.Drawing.Point(12, 366);
            this.showButton.Name = "showButton";
            this.showButton.Size = new System.Drawing.Size(75, 23);
            this.showButton.TabIndex = 0;
            this.showButton.Text = "Show";
            this.showButton.UseVisualStyleBackColor = true;
            this.showButton.Click += new System.EventHandler(this.showButton_Click);
            // 
            // fettPicture
            // 
            this.fettPicture.Image = ((System.Drawing.Image)(resources.GetObject("fettPicture.Image")));
            this.fettPicture.Location = new System.Drawing.Point(12, 3);
            this.fettPicture.Name = "fettPicture";
            this.fettPicture.Size = new System.Drawing.Size(189, 266);
            this.fettPicture.SizeMode = System.Windows.Forms.PictureBoxSizeMode.AutoSize;
            this.fettPicture.TabIndex = 1;
            this.fettPicture.TabStop = false;
            this.fettPicture.MouseClick += new System.Windows.Forms.MouseEventHandler(this.fettPicture_MouseClick);
            // 
            // hideButton
            // 
            this.hideButton.Location = new System.Drawing.Point(102, 366);
            this.hideButton.Name = "hideButton";
            this.hideButton.Size = new System.Drawing.Size(75, 23);
            this.hideButton.TabIndex = 2;
            this.hideButton.Text = "Hide";
            this.hideButton.UseVisualStyleBackColor = true;
            this.hideButton.Click += new System.EventHandler(this.hideButton_Click);
            // 
            // fett2Picture
            // 
            this.fett2Picture.Image = ((System.Drawing.Image)(resources.GetObject("fett2Picture.Image")));
            this.fett2Picture.Location = new System.Drawing.Point(254, 3);
            this.fett2Picture.Name = "fett2Picture";
            this.fett2Picture.Size = new System.Drawing.Size(189, 266);
            this.fett2Picture.SizeMode = System.Windows.Forms.PictureBoxSizeMode.AutoSize;
            this.fett2Picture.TabIndex = 4;
            this.fett2Picture.TabStop = false;
            this.fett2Picture.MouseClick += new System.Windows.Forms.MouseEventHandler(this.fettPicture_MouseClick);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("$this.BackgroundImage")));
            this.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.ClientSize = new System.Drawing.Size(710, 401);
            this.Controls.Add(this.fett2Picture);
            this.Controls.Add(this.hideButton);
            this.Controls.Add(this.fettPicture);
            this.Controls.Add(this.showButton);
            this.Name = "Form1";
            this.Text = "Form1";
            ((System.ComponentModel.ISupportInitialize)(this.fettPicture)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.fett2Picture)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button showButton;
        private System.Windows.Forms.PictureBox fettPicture;
        private System.Windows.Forms.Button hideButton;
        private System.Windows.Forms.PictureBox fett2Picture;
    }
}

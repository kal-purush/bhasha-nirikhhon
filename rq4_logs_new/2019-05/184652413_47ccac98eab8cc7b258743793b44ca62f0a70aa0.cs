namespace _21Ochko
{
    partial class MainMainMenu
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
            this.ChoseGameLabel = new System.Windows.Forms.Label();
            this.Ochko21button = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // ChoseGameLabel
            // 
            this.ChoseGameLabel.AutoSize = true;
            this.ChoseGameLabel.Location = new System.Drawing.Point(121, 27);
            this.ChoseGameLabel.Name = "ChoseGameLabel";
            this.ChoseGameLabel.Size = new System.Drawing.Size(82, 13);
            this.ChoseGameLabel.TabIndex = 0;
            this.ChoseGameLabel.Text = "Выберите игру";
            // 
            // Ochko21button
            // 
            this.Ochko21button.Location = new System.Drawing.Point(124, 146);
            this.Ochko21button.Name = "Ochko21button";
            this.Ochko21button.Size = new System.Drawing.Size(75, 23);
            this.Ochko21button.TabIndex = 1;
            this.Ochko21button.Text = "21Очко";
            this.Ochko21button.UseVisualStyleBackColor = true;
            this.Ochko21button.Click += new System.EventHandler(this.Ochko21button_Click);
            // 
            // MainMainMenu
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(313, 495);
            this.Controls.Add(this.Ochko21button);
            this.Controls.Add(this.ChoseGameLabel);
            this.Name = "MainMainMenu";
            this.Text = "Form1";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label ChoseGameLabel;
        private System.Windows.Forms.Button Ochko21button;
    }
}
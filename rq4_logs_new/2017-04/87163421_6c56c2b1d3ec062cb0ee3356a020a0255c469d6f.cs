namespace Lokaverkefni
{
    partial class Adalform
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
            this.label1 = new System.Windows.Forms.Label();
            this.lblwhoIsPlaying = new System.Windows.Forms.Label();
            this.btnTypingGame = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(12, 24);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(75, 13);
            this.label1.TabIndex = 0;
            this.label1.Text = "Choose game:";
            // 
            // lblwhoIsPlaying
            // 
            this.lblwhoIsPlaying.AutoSize = true;
            this.lblwhoIsPlaying.Location = new System.Drawing.Point(236, 24);
            this.lblwhoIsPlaying.Name = "lblwhoIsPlaying";
            this.lblwhoIsPlaying.Size = new System.Drawing.Size(97, 13);
            this.lblwhoIsPlaying.TabIndex = 4;
            this.lblwhoIsPlaying.Text = "You are playing as:";
            // 
            // btnTypingGame
            // 
            this.btnTypingGame.BackColor = System.Drawing.Color.White;
            this.btnTypingGame.BackgroundImage = global::Lokaverkefni.Properties.Resources.typing;
            this.btnTypingGame.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.btnTypingGame.Location = new System.Drawing.Point(12, 49);
            this.btnTypingGame.Name = "btnTypingGame";
            this.btnTypingGame.Size = new System.Drawing.Size(209, 93);
            this.btnTypingGame.TabIndex = 1;
            this.btnTypingGame.UseVisualStyleBackColor = false;
            // 
            // Adalform
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(441, 218);
            this.Controls.Add(this.lblwhoIsPlaying);
            this.Controls.Add(this.btnTypingGame);
            this.Controls.Add(this.label1);
            this.Name = "Adalform";
            this.Text = "Games";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Button btnTypingGame;
        private System.Windows.Forms.Label lblwhoIsPlaying;

    }
}
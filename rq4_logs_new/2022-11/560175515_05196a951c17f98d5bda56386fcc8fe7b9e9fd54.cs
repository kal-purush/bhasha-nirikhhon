
namespace IELTSpeaking
{
    partial class IELTSpeaking
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
            this.questions = new System.Windows.Forms.TextBox();
            this.SuspendLayout();
            // 
            // questions
            // 
            this.questions.Location = new System.Drawing.Point(304, 58);
            this.questions.Multiline = true;
            this.questions.Name = "questions";
            this.questions.Size = new System.Drawing.Size(695, 373);
            this.questions.TabIndex = 0;
            // 
            // IELTSpeaking
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1313, 499);
            this.Controls.Add(this.questions);
            this.Name = "IELTSpeaking";
            this.Text = "IELTSpeaking";
            this.Load += new System.EventHandler(this.IELTSpeaking_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox questions;
    }
}

namespace HeadSoccer.Screens
{
    partial class CharacterScreen
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

        #region Component Designer generated code

        /// <summary> 
        /// Required method for Designer support - do not modify 
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            this.Char1 = new System.Windows.Forms.PictureBox();
            this.Char2 = new System.Windows.Forms.PictureBox();
            this.updateTimer = new System.Windows.Forms.Timer(this.components);
            ((System.ComponentModel.ISupportInitialize)(this.Char1)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.Char2)).BeginInit();
            this.SuspendLayout();
            // 
            // Char1
            // 
            this.Char1.BackColor = System.Drawing.Color.Transparent;
            this.Char1.BackgroundImage = global::HeadSoccer.Properties.Resources.Chufu_Cry_Head;
            this.Char1.Location = new System.Drawing.Point(257, 132);
            this.Char1.Name = "Char1";
            this.Char1.Size = new System.Drawing.Size(120, 325);
            this.Char1.SizeMode = System.Windows.Forms.PictureBoxSizeMode.AutoSize;
            this.Char1.TabIndex = 0;
            this.Char1.TabStop = false;
            // 
            // Char2
            // 
            this.Char2.BackColor = System.Drawing.Color.Transparent;
            this.Char2.BackgroundImage = global::HeadSoccer.Properties.Resources.Chufu_Cry_Head;
            this.Char2.Location = new System.Drawing.Point(721, 132);
            this.Char2.Name = "Char2";
            this.Char2.Size = new System.Drawing.Size(120, 325);
            this.Char2.SizeMode = System.Windows.Forms.PictureBoxSizeMode.AutoSize;
            this.Char2.TabIndex = 1;
            this.Char2.TabStop = false;
            // 
            // updateTimer
            // 
            this.updateTimer.Enabled = true;
            this.updateTimer.Tick += new System.EventHandler(this.updateTimer_Tick);
            // 
            // CharacterScreen
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.Color.Firebrick;
            this.Controls.Add(this.Char2);
            this.Controls.Add(this.Char1);
            this.Name = "CharacterScreen";
            this.Size = new System.Drawing.Size(1106, 720);
            this.KeyUp += new System.Windows.Forms.KeyEventHandler(this.CharacterScreen_KeyUp);
            this.PreviewKeyDown += new System.Windows.Forms.PreviewKeyDownEventHandler(this.CharacterScreen_PreviewKeyDown);
            ((System.ComponentModel.ISupportInitialize)(this.Char1)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.Char2)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.PictureBox Char1;
        private System.Windows.Forms.PictureBox Char2;
        private System.Windows.Forms.Timer updateTimer;
    }
}
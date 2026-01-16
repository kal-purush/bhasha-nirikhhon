namespace ProcessingEngine.Algorithm.ExampleImplementation
{
    partial class UserControl1
    {
        /// <summary> 
        /// Erforderliche Designervariable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary> 
        /// Verwendete Ressourcen bereinigen.
        /// </summary>
        /// <param name="disposing">True, wenn verwaltete Ressourcen gelöscht werden sollen; andernfalls False.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Vom Komponenten-Designer generierter Code

        /// <summary> 
        /// Erforderliche Methode für die Designerunterstützung. 
        /// Der Inhalt der Methode darf nicht mit dem Code-Editor geändert werden.
        /// </summary>
        private void InitializeComponent()
        {
            this.txtLoop = new System.Windows.Forms.TextBox();
            this.lblLoop = new System.Windows.Forms.Label();
            this.lblSleep = new System.Windows.Forms.Label();
            this.txtSleep = new System.Windows.Forms.TextBox();
            this.SuspendLayout();
            // 
            // txtLoop
            // 
            this.txtLoop.Location = new System.Drawing.Point(77, 15);
            this.txtLoop.Name = "txtLoop";
            this.txtLoop.Size = new System.Drawing.Size(100, 20);
            this.txtLoop.TabIndex = 0;
            // 
            // lblLoop
            // 
            this.lblLoop.AutoSize = true;
            this.lblLoop.Location = new System.Drawing.Point(3, 15);
            this.lblLoop.Name = "lblLoop";
            this.lblLoop.Size = new System.Drawing.Size(68, 13);
            this.lblLoop.TabIndex = 1;
            this.lblLoop.Text = "LoopNumber";
            // 
            // lblSleep
            // 
            this.lblSleep.AutoSize = true;
            this.lblSleep.Location = new System.Drawing.Point(3, 44);
            this.lblSleep.Name = "lblSleep";
            this.lblSleep.Size = new System.Drawing.Size(57, 13);
            this.lblSleep.TabIndex = 2;
            this.lblSleep.Text = "SleepTime";
            // 
            // txtSleep
            // 
            this.txtSleep.Location = new System.Drawing.Point(77, 44);
            this.txtSleep.Name = "txtSleep";
            this.txtSleep.Size = new System.Drawing.Size(100, 20);
            this.txtSleep.TabIndex = 3;
            // 
            // UserControl1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.txtSleep);
            this.Controls.Add(this.lblSleep);
            this.Controls.Add(this.lblLoop);
            this.Controls.Add(this.txtLoop);
            this.Name = "UserControl1";
            this.Size = new System.Drawing.Size(246, 568);
            this.Load += new System.EventHandler(this.UserControl1_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        public System.Windows.Forms.TextBox txtLoop;
        private System.Windows.Forms.Label lblLoop;
        private System.Windows.Forms.Label lblSleep;
        public System.Windows.Forms.TextBox txtSleep;
    }
}
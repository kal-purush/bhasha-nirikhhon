namespace ICT4Rails.Forms
{
    partial class SimulationForm
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
            this.pl_Form_Total_Context = new System.Windows.Forms.Panel();
            this.panel3 = new System.Windows.Forms.Panel();
            this.label1 = new System.Windows.Forms.Label();
            this.pStatusOverview = new System.Windows.Forms.Panel();
            this.pbProgress = new System.Windows.Forms.ProgressBar();
            this.lbInfo = new System.Windows.Forms.Label();
            this.pl_Form_Total_Context.SuspendLayout();
            this.panel3.SuspendLayout();
            this.pStatusOverview.SuspendLayout();
            this.SuspendLayout();
            // 
            // pl_Form_Total_Context
            // 
            this.pl_Form_Total_Context.Controls.Add(this.panel3);
            this.pl_Form_Total_Context.Controls.Add(this.pStatusOverview);
            this.pl_Form_Total_Context.Location = new System.Drawing.Point(3, 3);
            this.pl_Form_Total_Context.Name = "pl_Form_Total_Context";
            this.pl_Form_Total_Context.Size = new System.Drawing.Size(441, 359);
            this.pl_Form_Total_Context.TabIndex = 1;
            // 
            // panel3
            // 
            this.panel3.BackColor = System.Drawing.SystemColors.Control;
            this.panel3.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.panel3.Controls.Add(this.label1);
            this.panel3.Location = new System.Drawing.Point(3, 3);
            this.panel3.Name = "panel3";
            this.panel3.Size = new System.Drawing.Size(435, 81);
            this.panel3.TabIndex = 3;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 26.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.Location = new System.Drawing.Point(98, 20);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(231, 39);
            this.label1.TabIndex = 0;
            this.label1.Text = "EyeCT4Rails";
            // 
            // pStatusOverview
            // 
            this.pStatusOverview.BackColor = System.Drawing.SystemColors.ButtonHighlight;
            this.pStatusOverview.Controls.Add(this.lbInfo);
            this.pStatusOverview.Controls.Add(this.pbProgress);
            this.pStatusOverview.Location = new System.Drawing.Point(3, 90);
            this.pStatusOverview.Name = "pStatusOverview";
            this.pStatusOverview.Size = new System.Drawing.Size(434, 266);
            this.pStatusOverview.TabIndex = 7;
            // 
            // pbProgress
            // 
            this.pbProgress.Location = new System.Drawing.Point(72, 118);
            this.pbProgress.Name = "pbProgress";
            this.pbProgress.Size = new System.Drawing.Size(294, 23);
            this.pbProgress.TabIndex = 0;
            // 
            // lbInfo
            // 
            this.lbInfo.AutoSize = true;
            this.lbInfo.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F);
            this.lbInfo.Location = new System.Drawing.Point(161, 144);
            this.lbInfo.Name = "lbInfo";
            this.lbInfo.Size = new System.Drawing.Size(116, 16);
            this.lbInfo.TabIndex = 29;
            this.lbInfo.Text = " Van de    gedaan;";
            // 
            // SimulationForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.SystemColors.AppWorkspace;
            this.ClientSize = new System.Drawing.Size(446, 365);
            this.Controls.Add(this.pl_Form_Total_Context);
            this.MaximumSize = new System.Drawing.Size(462, 404);
            this.Name = "SimulationForm";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Progress";
            this.Resize += new System.EventHandler(this.SimulationForm_Resize);
            this.pl_Form_Total_Context.ResumeLayout(false);
            this.panel3.ResumeLayout(false);
            this.panel3.PerformLayout();
            this.pStatusOverview.ResumeLayout(false);
            this.pStatusOverview.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Panel pl_Form_Total_Context;
        private System.Windows.Forms.Panel panel3;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Panel pStatusOverview;
        private System.Windows.Forms.ProgressBar pbProgress;
        private System.Windows.Forms.Label lbInfo;
    }
}
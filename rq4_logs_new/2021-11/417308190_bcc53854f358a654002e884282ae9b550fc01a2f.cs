
namespace PipelineSimulation.WinForm
{
    partial class FPUView
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
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.txtStack1 = new System.Windows.Forms.TextBox();
            this.txtStack2 = new System.Windows.Forms.TextBox();
            this.txtStack3 = new System.Windows.Forms.TextBox();
            this.txtStack4 = new System.Windows.Forms.TextBox();
            this.txtStack5 = new System.Windows.Forms.TextBox();
            this.txtStack6 = new System.Windows.Forms.TextBox();
            this.txtStack7 = new System.Windows.Forms.TextBox();
            this.txtStack8 = new System.Windows.Forms.TextBox();
            this.lblTop = new System.Windows.Forms.Label();
            this.groupBox1.SuspendLayout();
            this.SuspendLayout();
            // 
            // groupBox1
            // 
            this.groupBox1.Controls.Add(this.txtStack8);
            this.groupBox1.Controls.Add(this.txtStack7);
            this.groupBox1.Controls.Add(this.txtStack6);
            this.groupBox1.Controls.Add(this.txtStack5);
            this.groupBox1.Controls.Add(this.txtStack4);
            this.groupBox1.Controls.Add(this.txtStack3);
            this.groupBox1.Controls.Add(this.txtStack2);
            this.groupBox1.Controls.Add(this.txtStack1);
            this.groupBox1.Location = new System.Drawing.Point(123, 13);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Size = new System.Drawing.Size(200, 258);
            this.groupBox1.TabIndex = 0;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "FPU Stack";
            // 
            // txtStack1
            // 
            this.txtStack1.Location = new System.Drawing.Point(6, 22);
            this.txtStack1.Name = "txtStack1";
            this.txtStack1.ReadOnly = true;
            this.txtStack1.Size = new System.Drawing.Size(187, 23);
            this.txtStack1.TabIndex = 0;
            // 
            // txtStack2
            // 
            this.txtStack2.Location = new System.Drawing.Point(6, 52);
            this.txtStack2.Name = "txtStack2";
            this.txtStack2.ReadOnly = true;
            this.txtStack2.Size = new System.Drawing.Size(187, 23);
            this.txtStack2.TabIndex = 1;
            // 
            // txtStack3
            // 
            this.txtStack3.Location = new System.Drawing.Point(6, 81);
            this.txtStack3.Name = "txtStack3";
            this.txtStack3.ReadOnly = true;
            this.txtStack3.Size = new System.Drawing.Size(187, 23);
            this.txtStack3.TabIndex = 2;
            // 
            // txtStack4
            // 
            this.txtStack4.Location = new System.Drawing.Point(6, 110);
            this.txtStack4.Name = "txtStack4";
            this.txtStack4.ReadOnly = true;
            this.txtStack4.Size = new System.Drawing.Size(187, 23);
            this.txtStack4.TabIndex = 3;
            // 
            // txtStack5
            // 
            this.txtStack5.Location = new System.Drawing.Point(6, 139);
            this.txtStack5.Name = "txtStack5";
            this.txtStack5.ReadOnly = true;
            this.txtStack5.Size = new System.Drawing.Size(187, 23);
            this.txtStack5.TabIndex = 4;
            // 
            // txtStack6
            // 
            this.txtStack6.Location = new System.Drawing.Point(6, 168);
            this.txtStack6.Name = "txtStack6";
            this.txtStack6.ReadOnly = true;
            this.txtStack6.Size = new System.Drawing.Size(187, 23);
            this.txtStack6.TabIndex = 5;
            // 
            // txtStack7
            // 
            this.txtStack7.Location = new System.Drawing.Point(6, 197);
            this.txtStack7.Name = "txtStack7";
            this.txtStack7.ReadOnly = true;
            this.txtStack7.Size = new System.Drawing.Size(187, 23);
            this.txtStack7.TabIndex = 6;
            // 
            // txtStack8
            // 
            this.txtStack8.Location = new System.Drawing.Point(6, 226);
            this.txtStack8.Name = "txtStack8";
            this.txtStack8.ReadOnly = true;
            this.txtStack8.Size = new System.Drawing.Size(187, 23);
            this.txtStack8.TabIndex = 7;
            // 
            // lblTop
            // 
            this.lblTop.AutoSize = true;
            this.lblTop.Location = new System.Drawing.Point(12, 37);
            this.lblTop.Name = "lblTop";
            this.lblTop.Size = new System.Drawing.Size(31, 15);
            this.lblTop.TabIndex = 1;
            this.lblTop.Text = "TOP:";
            // 
            // FPUView
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(7F, 15F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(334, 283);
            this.Controls.Add(this.lblTop);
            this.Controls.Add(this.groupBox1);
            this.Name = "FPUView";
            this.Text = "FPUView";
            this.Load += new System.EventHandler(this.FPUView_Load);
            this.groupBox1.ResumeLayout(false);
            this.groupBox1.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.TextBox txtStack4;
        private System.Windows.Forms.TextBox txtStack3;
        private System.Windows.Forms.TextBox txtStack2;
        private System.Windows.Forms.TextBox txtStack1;
        private System.Windows.Forms.TextBox txtStack8;
        private System.Windows.Forms.TextBox txtStack7;
        private System.Windows.Forms.TextBox txtStack6;
        private System.Windows.Forms.TextBox txtStack5;
        private System.Windows.Forms.Label lblTop;
    }
}
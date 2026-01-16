namespace finview.Controls
{
    partial class ucMonthRoller
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
            this.btnbackward = new System.Windows.Forms.Button();
            this.btnforward = new System.Windows.Forms.Button();
            this.lblActiveMonth = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // btnbackward
            // 
            this.btnbackward.Location = new System.Drawing.Point(3, 1);
            this.btnbackward.Name = "btnbackward";
            this.btnbackward.Size = new System.Drawing.Size(29, 22);
            this.btnbackward.TabIndex = 0;
            this.btnbackward.Text = "<<";
            this.btnbackward.UseVisualStyleBackColor = true;
            this.btnbackward.Click += new System.EventHandler(this.btnbackward_Click);
            // 
            // btnforward
            // 
            this.btnforward.Location = new System.Drawing.Point(103, 2);
            this.btnforward.Name = "btnforward";
            this.btnforward.Size = new System.Drawing.Size(28, 21);
            this.btnforward.TabIndex = 1;
            this.btnforward.Text = ">>";
            this.btnforward.UseVisualStyleBackColor = true;
            this.btnforward.Click += new System.EventHandler(this.btnforward_Click);
            // 
            // lblActiveMonth
            // 
            this.lblActiveMonth.AutoSize = true;
            this.lblActiveMonth.ForeColor = System.Drawing.Color.Green;
            this.lblActiveMonth.Location = new System.Drawing.Point(45, 5);
            this.lblActiveMonth.Name = "lblActiveMonth";
            this.lblActiveMonth.Size = new System.Drawing.Size(10, 13);
            this.lblActiveMonth.TabIndex = 2;
            this.lblActiveMonth.Text = ".";
            // 
            // ucMonthRoller
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.lblActiveMonth);
            this.Controls.Add(this.btnforward);
            this.Controls.Add(this.btnbackward);
            this.Name = "ucMonthRoller";
            this.Size = new System.Drawing.Size(134, 22);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button btnbackward;
        private System.Windows.Forms.Button btnforward;
        private System.Windows.Forms.Label lblActiveMonth;
    }
}
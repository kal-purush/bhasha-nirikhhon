namespace Restaurant
{
    partial class HostForm
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
            this.submitCustomers = new System.Windows.Forms.Button();
            this.customerNumber = new System.Windows.Forms.TextBox();
            this.label1 = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // submitCustomers
            // 
            this.submitCustomers.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F);
            this.submitCustomers.Location = new System.Drawing.Point(74, 100);
            this.submitCustomers.Name = "submitCustomers";
            this.submitCustomers.Size = new System.Drawing.Size(142, 37);
            this.submitCustomers.TabIndex = 0;
            this.submitCustomers.Text = "Submit";
            this.submitCustomers.UseVisualStyleBackColor = true;
            this.submitCustomers.Click += new System.EventHandler(this.submitCustomers_Click);
            // 
            // customerNumber
            // 
            this.customerNumber.Location = new System.Drawing.Point(74, 59);
            this.customerNumber.Name = "customerNumber";
            this.customerNumber.Size = new System.Drawing.Size(142, 20);
            this.customerNumber.TabIndex = 1;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.Location = new System.Drawing.Point(22, 26);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(238, 16);
            this.label1.TabIndex = 2;
            this.label1.Text = "How many customers are you seating?";
            // 
            // HostForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(284, 159);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.customerNumber);
            this.Controls.Add(this.submitCustomers);
            this.Name = "HostForm";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Form1";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button submitCustomers;
        private System.Windows.Forms.TextBox customerNumber;
        private System.Windows.Forms.Label label1;
    }
}

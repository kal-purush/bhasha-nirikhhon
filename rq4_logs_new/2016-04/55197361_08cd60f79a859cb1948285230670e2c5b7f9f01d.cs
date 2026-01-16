namespace Restaurant
{
    partial class CashOutForm
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
            this.cbTableList = new System.Windows.Forms.ComboBox();
            this.lblSelectTable = new System.Windows.Forms.Label();
            this.btnPrev = new System.Windows.Forms.Button();
            this.btnCashOut = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // cbTableList
            // 
            this.cbTableList.FormattingEnabled = true;
            this.cbTableList.Items.AddRange(new object[] {
            "Table 1",
            "Table 2",
            "Table 3",
            "Table 4",
            "Table 5",
            "Table 6",
            "Table 7",
            "Table 8",
            "Table 9",
            "Table 10"});
            this.cbTableList.Location = new System.Drawing.Point(66, 92);
            this.cbTableList.Name = "cbTableList";
            this.cbTableList.Size = new System.Drawing.Size(157, 21);
            this.cbTableList.TabIndex = 0;
            // 
            // lblSelectTable
            // 
            this.lblSelectTable.AutoSize = true;
            this.lblSelectTable.Font = new System.Drawing.Font("Arial Narrow", 14.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblSelectTable.Location = new System.Drawing.Point(86, 45);
            this.lblSelectTable.Name = "lblSelectTable";
            this.lblSelectTable.Size = new System.Drawing.Size(103, 23);
            this.lblSelectTable.TabIndex = 1;
            this.lblSelectTable.Text = "Select Table :";
            // 
            // btnPrev
            // 
            this.btnPrev.Location = new System.Drawing.Point(66, 147);
            this.btnPrev.Name = "btnPrev";
            this.btnPrev.Size = new System.Drawing.Size(75, 23);
            this.btnPrev.TabIndex = 2;
            this.btnPrev.Text = "Previous <";
            this.btnPrev.UseVisualStyleBackColor = true;
            this.btnPrev.Click += new System.EventHandler(this.btnPrev_Click);
            // 
            // btnCashOut
            // 
            this.btnCashOut.Location = new System.Drawing.Point(147, 147);
            this.btnCashOut.Name = "btnCashOut";
            this.btnCashOut.Size = new System.Drawing.Size(75, 23);
            this.btnCashOut.TabIndex = 2;
            this.btnCashOut.Text = "Cash Out";
            this.btnCashOut.UseVisualStyleBackColor = true;
            this.btnCashOut.Click += new System.EventHandler(this.btnCashOut_Click);
            // 
            // CashOutForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(284, 235);
            this.Controls.Add(this.btnCashOut);
            this.Controls.Add(this.btnPrev);
            this.Controls.Add(this.lblSelectTable);
            this.Controls.Add(this.cbTableList);
            this.Name = "CashOutForm";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "CashOutForm";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.ComboBox cbTableList;
        private System.Windows.Forms.Label lblSelectTable;
        private System.Windows.Forms.Button btnPrev;
        private System.Windows.Forms.Button btnCashOut;
    }
}
namespace AuctionNet.Forms
{
    partial class FrmCustomerList
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
            this.listViewCustomers = new System.Windows.Forms.ListView();
            this.SuspendLayout();
            // 
            // listViewCustomers
            // 
            this.listViewCustomers.Location = new System.Drawing.Point(0, 0);
            this.listViewCustomers.Name = "listViewCustomers";
            this.listViewCustomers.Size = new System.Drawing.Size(475, 388);
            this.listViewCustomers.TabIndex = 0;
            this.listViewCustomers.UseCompatibleStateImageBehavior = false;
            // 
            // FrmCustomerList
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(474, 385);
            this.Controls.Add(this.listViewCustomers);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedSingle;
            this.Name = "FrmCustomerList";
            this.Text = "Kundlista";
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.ListView listViewCustomers;
    }
}
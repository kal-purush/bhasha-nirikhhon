namespace AuctionNet.Forms
{
    partial class FrmAddAuctin
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
            this.lblStartTime = new System.Windows.Forms.Label();
            this.txtStartTime = new System.Windows.Forms.TextBox();
            this.textBox1 = new System.Windows.Forms.TextBox();
            this.lblEndTime = new System.Windows.Forms.Label();
            this.txtAcceptPrice = new System.Windows.Forms.TextBox();
            this.lblAcceptPrice = new System.Windows.Forms.Label();
            this.txtProductId = new System.Windows.Forms.TextBox();
            this.lblProductId = new System.Windows.Forms.Label();
            this.btnAddAuction = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // lblStartTime
            // 
            this.lblStartTime.AutoSize = true;
            this.lblStartTime.Location = new System.Drawing.Point(23, 26);
            this.lblStartTime.Name = "lblStartTime";
            this.lblStartTime.Size = new System.Drawing.Size(53, 17);
            this.lblStartTime.TabIndex = 0;
            this.lblStartTime.Text = "Starttid";
            // 
            // txtStartTime
            // 
            this.txtStartTime.Location = new System.Drawing.Point(26, 60);
            this.txtStartTime.Name = "txtStartTime";
            this.txtStartTime.Size = new System.Drawing.Size(161, 22);
            this.txtStartTime.TabIndex = 1;
            // 
            // textBox1
            // 
            this.textBox1.Location = new System.Drawing.Point(26, 142);
            this.textBox1.Name = "textBox1";
            this.textBox1.Size = new System.Drawing.Size(161, 22);
            this.textBox1.TabIndex = 3;
            // 
            // lblEndTime
            // 
            this.lblEndTime.AutoSize = true;
            this.lblEndTime.Location = new System.Drawing.Point(23, 108);
            this.lblEndTime.Name = "lblEndTime";
            this.lblEndTime.Size = new System.Drawing.Size(47, 17);
            this.lblEndTime.TabIndex = 2;
            this.lblEndTime.Text = "Sluttid";
            // 
            // txtAcceptPrice
            // 
            this.txtAcceptPrice.Location = new System.Drawing.Point(246, 60);
            this.txtAcceptPrice.Name = "txtAcceptPrice";
            this.txtAcceptPrice.Size = new System.Drawing.Size(109, 22);
            this.txtAcceptPrice.TabIndex = 5;
            // 
            // lblAcceptPrice
            // 
            this.lblAcceptPrice.AutoSize = true;
            this.lblAcceptPrice.Location = new System.Drawing.Point(243, 26);
            this.lblAcceptPrice.Name = "lblAcceptPrice";
            this.lblAcceptPrice.Size = new System.Drawing.Size(74, 17);
            this.lblAcceptPrice.TabIndex = 4;
            this.lblAcceptPrice.Text = "Acceptpris";
            // 
            // txtProductId
            // 
            this.txtProductId.Location = new System.Drawing.Point(246, 142);
            this.txtProductId.Name = "txtProductId";
            this.txtProductId.Size = new System.Drawing.Size(109, 22);
            this.txtProductId.TabIndex = 7;
            // 
            // lblProductId
            // 
            this.lblProductId.AutoSize = true;
            this.lblProductId.Location = new System.Drawing.Point(243, 108);
            this.lblProductId.Name = "lblProductId";
            this.lblProductId.Size = new System.Drawing.Size(68, 17);
            this.lblProductId.TabIndex = 6;
            this.lblProductId.Text = "ProduktId";
            // 
            // btnAddAuction
            // 
            this.btnAddAuction.DialogResult = System.Windows.Forms.DialogResult.No;
            this.btnAddAuction.Location = new System.Drawing.Point(26, 207);
            this.btnAddAuction.Name = "btnAddAuction";
            this.btnAddAuction.Size = new System.Drawing.Size(132, 45);
            this.btnAddAuction.TabIndex = 8;
            this.btnAddAuction.Text = "Spara Auktion";
            this.btnAddAuction.UseVisualStyleBackColor = true;
            // 
            // FrmAddAuctin
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(395, 282);
            this.Controls.Add(this.btnAddAuction);
            this.Controls.Add(this.txtProductId);
            this.Controls.Add(this.lblProductId);
            this.Controls.Add(this.txtAcceptPrice);
            this.Controls.Add(this.lblAcceptPrice);
            this.Controls.Add(this.textBox1);
            this.Controls.Add(this.lblEndTime);
            this.Controls.Add(this.txtStartTime);
            this.Controls.Add(this.lblStartTime);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedSingle;
            this.Name = "FrmAddAuctin";
            this.Text = "Lägg till Auktion";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label lblStartTime;
        private System.Windows.Forms.TextBox txtStartTime;
        private System.Windows.Forms.TextBox textBox1;
        private System.Windows.Forms.Label lblEndTime;
        private System.Windows.Forms.TextBox txtAcceptPrice;
        private System.Windows.Forms.Label lblAcceptPrice;
        private System.Windows.Forms.TextBox txtProductId;
        private System.Windows.Forms.Label lblProductId;
        private System.Windows.Forms.Button btnAddAuction;
    }
}
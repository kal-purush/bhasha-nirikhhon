namespace NutsBoltsandBeyondHardwareStore
{
    partial class frmCustomerAccountInfo
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
            this.btnClose = new System.Windows.Forms.Button();
            this.lblCustomerID = new System.Windows.Forms.Label();
            this.lblFirstName = new System.Windows.Forms.Label();
            this.gbxBasicInfo = new System.Windows.Forms.GroupBox();
            this.gbxPaymentInfo = new System.Windows.Forms.GroupBox();
            this.btnSaveInfo = new System.Windows.Forms.Button();
            this.lblLastName = new System.Windows.Forms.Label();
            this.label4 = new System.Windows.Forms.Label();
            this.lblZip = new System.Windows.Forms.Label();
            this.lblCreditCardNum = new System.Windows.Forms.Label();
            this.lblExpDate = new System.Windows.Forms.Label();
            this.lblCCV = new System.Windows.Forms.Label();
            this.textBox1 = new System.Windows.Forms.TextBox();
            this.textBox2 = new System.Windows.Forms.TextBox();
            this.textBox3 = new System.Windows.Forms.TextBox();
            this.textBox4 = new System.Windows.Forms.TextBox();
            this.textBox5 = new System.Windows.Forms.TextBox();
            this.textBox6 = new System.Windows.Forms.TextBox();
            this.textBox7 = new System.Windows.Forms.TextBox();
            this.textBox8 = new System.Windows.Forms.TextBox();
            this.btnEdit = new System.Windows.Forms.Button();
            this.gbxBasicInfo.SuspendLayout();
            this.gbxPaymentInfo.SuspendLayout();
            this.SuspendLayout();
            // 
            // btnClose
            // 
            this.btnClose.Location = new System.Drawing.Point(382, 188);
            this.btnClose.Name = "btnClose";
            this.btnClose.Size = new System.Drawing.Size(99, 23);
            this.btnClose.TabIndex = 0;
            this.btnClose.Text = "Close Window";
            this.btnClose.UseVisualStyleBackColor = true;
            // 
            // lblCustomerID
            // 
            this.lblCustomerID.AutoSize = true;
            this.lblCustomerID.Location = new System.Drawing.Point(27, 32);
            this.lblCustomerID.Name = "lblCustomerID";
            this.lblCustomerID.Size = new System.Drawing.Size(65, 13);
            this.lblCustomerID.TabIndex = 1;
            this.lblCustomerID.Text = "Customer ID";
            // 
            // lblFirstName
            // 
            this.lblFirstName.AutoSize = true;
            this.lblFirstName.Location = new System.Drawing.Point(35, 61);
            this.lblFirstName.Name = "lblFirstName";
            this.lblFirstName.Size = new System.Drawing.Size(57, 13);
            this.lblFirstName.TabIndex = 2;
            this.lblFirstName.Text = "First Name";
            // 
            // gbxBasicInfo
            // 
            this.gbxBasicInfo.Controls.Add(this.textBox5);
            this.gbxBasicInfo.Controls.Add(this.textBox4);
            this.gbxBasicInfo.Controls.Add(this.textBox3);
            this.gbxBasicInfo.Controls.Add(this.textBox2);
            this.gbxBasicInfo.Controls.Add(this.textBox1);
            this.gbxBasicInfo.Controls.Add(this.lblZip);
            this.gbxBasicInfo.Controls.Add(this.label4);
            this.gbxBasicInfo.Controls.Add(this.lblLastName);
            this.gbxBasicInfo.Controls.Add(this.lblFirstName);
            this.gbxBasicInfo.Controls.Add(this.lblCustomerID);
            this.gbxBasicInfo.Location = new System.Drawing.Point(31, 25);
            this.gbxBasicInfo.Name = "gbxBasicInfo";
            this.gbxBasicInfo.Size = new System.Drawing.Size(232, 184);
            this.gbxBasicInfo.TabIndex = 3;
            this.gbxBasicInfo.TabStop = false;
            this.gbxBasicInfo.Text = "Basic Info";
            // 
            // gbxPaymentInfo
            // 
            this.gbxPaymentInfo.Controls.Add(this.textBox6);
            this.gbxPaymentInfo.Controls.Add(this.textBox8);
            this.gbxPaymentInfo.Controls.Add(this.textBox7);
            this.gbxPaymentInfo.Controls.Add(this.lblCCV);
            this.gbxPaymentInfo.Controls.Add(this.lblExpDate);
            this.gbxPaymentInfo.Controls.Add(this.lblCreditCardNum);
            this.gbxPaymentInfo.Location = new System.Drawing.Point(302, 25);
            this.gbxPaymentInfo.Name = "gbxPaymentInfo";
            this.gbxPaymentInfo.Size = new System.Drawing.Size(262, 121);
            this.gbxPaymentInfo.TabIndex = 0;
            this.gbxPaymentInfo.TabStop = false;
            this.gbxPaymentInfo.Text = "Payment Info";
            // 
            // btnSaveInfo
            // 
            this.btnSaveInfo.Location = new System.Drawing.Point(460, 159);
            this.btnSaveInfo.Name = "btnSaveInfo";
            this.btnSaveInfo.Size = new System.Drawing.Size(75, 23);
            this.btnSaveInfo.TabIndex = 4;
            this.btnSaveInfo.Text = "Save";
            this.btnSaveInfo.UseVisualStyleBackColor = true;
            // 
            // lblLastName
            // 
            this.lblLastName.AutoSize = true;
            this.lblLastName.Location = new System.Drawing.Point(34, 82);
            this.lblLastName.Name = "lblLastName";
            this.lblLastName.Size = new System.Drawing.Size(58, 13);
            this.lblLastName.TabIndex = 3;
            this.lblLastName.Text = "Last Name";
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(47, 108);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(45, 13);
            this.label4.TabIndex = 4;
            this.label4.Text = "Address";
            // 
            // lblZip
            // 
            this.lblZip.AutoSize = true;
            this.lblZip.Location = new System.Drawing.Point(42, 134);
            this.lblZip.Name = "lblZip";
            this.lblZip.Size = new System.Drawing.Size(50, 13);
            this.lblZip.TabIndex = 5;
            this.lblZip.Text = "Zip Code";
            // 
            // lblCreditCardNum
            // 
            this.lblCreditCardNum.AutoSize = true;
            this.lblCreditCardNum.Location = new System.Drawing.Point(19, 35);
            this.lblCreditCardNum.Name = "lblCreditCardNum";
            this.lblCreditCardNum.Size = new System.Drawing.Size(69, 13);
            this.lblCreditCardNum.TabIndex = 6;
            this.lblCreditCardNum.Text = "Credit Card #";
            // 
            // lblExpDate
            // 
            this.lblExpDate.AutoSize = true;
            this.lblExpDate.Location = new System.Drawing.Point(37, 61);
            this.lblExpDate.Name = "lblExpDate";
            this.lblExpDate.Size = new System.Drawing.Size(51, 13);
            this.lblExpDate.TabIndex = 7;
            this.lblExpDate.Text = "Exp Date";
            // 
            // lblCCV
            // 
            this.lblCCV.AutoSize = true;
            this.lblCCV.Location = new System.Drawing.Point(60, 87);
            this.lblCCV.Name = "lblCCV";
            this.lblCCV.Size = new System.Drawing.Size(28, 13);
            this.lblCCV.TabIndex = 8;
            this.lblCCV.Text = "CCV";
            // 
            // textBox1
            // 
            this.textBox1.Enabled = false;
            this.textBox1.Location = new System.Drawing.Point(98, 27);
            this.textBox1.Name = "textBox1";
            this.textBox1.Size = new System.Drawing.Size(100, 20);
            this.textBox1.TabIndex = 6;
            // 
            // textBox2
            // 
            this.textBox2.Enabled = false;
            this.textBox2.Location = new System.Drawing.Point(98, 54);
            this.textBox2.Name = "textBox2";
            this.textBox2.Size = new System.Drawing.Size(100, 20);
            this.textBox2.TabIndex = 7;
            // 
            // textBox3
            // 
            this.textBox3.Enabled = false;
            this.textBox3.Location = new System.Drawing.Point(98, 79);
            this.textBox3.Name = "textBox3";
            this.textBox3.Size = new System.Drawing.Size(100, 20);
            this.textBox3.TabIndex = 8;
            // 
            // textBox4
            // 
            this.textBox4.Enabled = false;
            this.textBox4.Location = new System.Drawing.Point(98, 105);
            this.textBox4.Name = "textBox4";
            this.textBox4.Size = new System.Drawing.Size(100, 20);
            this.textBox4.TabIndex = 9;
            // 
            // textBox5
            // 
            this.textBox5.Enabled = false;
            this.textBox5.Location = new System.Drawing.Point(98, 131);
            this.textBox5.Name = "textBox5";
            this.textBox5.Size = new System.Drawing.Size(100, 20);
            this.textBox5.TabIndex = 10;
            // 
            // textBox6
            // 
            this.textBox6.Enabled = false;
            this.textBox6.Location = new System.Drawing.Point(94, 32);
            this.textBox6.Name = "textBox6";
            this.textBox6.Size = new System.Drawing.Size(139, 20);
            this.textBox6.TabIndex = 11;
            // 
            // textBox7
            // 
            this.textBox7.Enabled = false;
            this.textBox7.Location = new System.Drawing.Point(94, 58);
            this.textBox7.Name = "textBox7";
            this.textBox7.Size = new System.Drawing.Size(139, 20);
            this.textBox7.TabIndex = 12;
            // 
            // textBox8
            // 
            this.textBox8.Enabled = false;
            this.textBox8.Location = new System.Drawing.Point(94, 85);
            this.textBox8.MaxLength = 3;
            this.textBox8.Name = "textBox8";
            this.textBox8.Size = new System.Drawing.Size(53, 20);
            this.textBox8.TabIndex = 13;
            // 
            // btnEdit
            // 
            this.btnEdit.Location = new System.Drawing.Point(319, 159);
            this.btnEdit.Name = "btnEdit";
            this.btnEdit.Size = new System.Drawing.Size(75, 23);
            this.btnEdit.TabIndex = 11;
            this.btnEdit.Text = "Edit";
            this.btnEdit.UseVisualStyleBackColor = true;
            // 
            // frmCustomerAccountInfo
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(600, 236);
            this.ControlBox = false;
            this.Controls.Add(this.btnEdit);
            this.Controls.Add(this.btnSaveInfo);
            this.Controls.Add(this.gbxPaymentInfo);
            this.Controls.Add(this.gbxBasicInfo);
            this.Controls.Add(this.btnClose);
            this.Name = "frmCustomerAccountInfo";
            this.Text = "Account Info";
            this.gbxBasicInfo.ResumeLayout(false);
            this.gbxBasicInfo.PerformLayout();
            this.gbxPaymentInfo.ResumeLayout(false);
            this.gbxPaymentInfo.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Button btnClose;
        private System.Windows.Forms.Label lblCustomerID;
        private System.Windows.Forms.Label lblFirstName;
        private System.Windows.Forms.GroupBox gbxBasicInfo;
        private System.Windows.Forms.TextBox textBox5;
        private System.Windows.Forms.TextBox textBox4;
        private System.Windows.Forms.TextBox textBox3;
        private System.Windows.Forms.TextBox textBox2;
        private System.Windows.Forms.TextBox textBox1;
        private System.Windows.Forms.Label lblZip;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.Label lblLastName;
        private System.Windows.Forms.GroupBox gbxPaymentInfo;
        private System.Windows.Forms.TextBox textBox6;
        private System.Windows.Forms.TextBox textBox8;
        private System.Windows.Forms.TextBox textBox7;
        private System.Windows.Forms.Label lblCCV;
        private System.Windows.Forms.Label lblExpDate;
        private System.Windows.Forms.Label lblCreditCardNum;
        private System.Windows.Forms.Button btnSaveInfo;
        private System.Windows.Forms.Button btnEdit;
    }
}
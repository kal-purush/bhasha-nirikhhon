namespace OrderSystem
{
    partial class UpdateForm
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
            this.IDExist = new System.Windows.Forms.TextBox();
            this.FirstNameExist = new System.Windows.Forms.TextBox();
            this.LastNameExist = new System.Windows.Forms.TextBox();
            this.TelephoneExist = new System.Windows.Forms.TextBox();
            this.AddressExist = new System.Windows.Forms.TextBox();
            this.IDReplace = new System.Windows.Forms.TextBox();
            this.FirstNameReplace = new System.Windows.Forms.TextBox();
            this.LastNameReplace = new System.Windows.Forms.TextBox();
            this.TelephoneReplace = new System.Windows.Forms.TextBox();
            this.AddressReplace = new System.Windows.Forms.TextBox();
            this.button1 = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // IDExist
            // 
            this.IDExist.Location = new System.Drawing.Point(12, 13);
            this.IDExist.Name = "IDExist";
            this.IDExist.Size = new System.Drawing.Size(189, 20);
            this.IDExist.TabIndex = 0;
            // 
            // FirstNameExist
            // 
            this.FirstNameExist.Location = new System.Drawing.Point(12, 39);
            this.FirstNameExist.Name = "FirstNameExist";
            this.FirstNameExist.Size = new System.Drawing.Size(189, 20);
            this.FirstNameExist.TabIndex = 1;
            // 
            // LastNameExist
            // 
            this.LastNameExist.Location = new System.Drawing.Point(12, 65);
            this.LastNameExist.Name = "LastNameExist";
            this.LastNameExist.Size = new System.Drawing.Size(189, 20);
            this.LastNameExist.TabIndex = 2;
            // 
            // TelephoneExist
            // 
            this.TelephoneExist.Location = new System.Drawing.Point(12, 91);
            this.TelephoneExist.Name = "TelephoneExist";
            this.TelephoneExist.Size = new System.Drawing.Size(189, 20);
            this.TelephoneExist.TabIndex = 3;
            // 
            // AddressExist
            // 
            this.AddressExist.Location = new System.Drawing.Point(12, 117);
            this.AddressExist.Name = "AddressExist";
            this.AddressExist.Size = new System.Drawing.Size(189, 20);
            this.AddressExist.TabIndex = 4;
            // 
            // IDReplace
            // 
            this.IDReplace.Location = new System.Drawing.Point(231, 12);
            this.IDReplace.Name = "IDReplace";
            this.IDReplace.Size = new System.Drawing.Size(189, 20);
            this.IDReplace.TabIndex = 5;
            // 
            // FirstNameReplace
            // 
            this.FirstNameReplace.Location = new System.Drawing.Point(231, 39);
            this.FirstNameReplace.Name = "FirstNameReplace";
            this.FirstNameReplace.Size = new System.Drawing.Size(189, 20);
            this.FirstNameReplace.TabIndex = 6;
            // 
            // LastNameReplace
            // 
            this.LastNameReplace.Location = new System.Drawing.Point(231, 65);
            this.LastNameReplace.Name = "LastNameReplace";
            this.LastNameReplace.Size = new System.Drawing.Size(189, 20);
            this.LastNameReplace.TabIndex = 7;
            // 
            // TelephoneReplace
            // 
            this.TelephoneReplace.Location = new System.Drawing.Point(231, 91);
            this.TelephoneReplace.Name = "TelephoneReplace";
            this.TelephoneReplace.Size = new System.Drawing.Size(189, 20);
            this.TelephoneReplace.TabIndex = 8;
            // 
            // AddressReplace
            // 
            this.AddressReplace.Location = new System.Drawing.Point(231, 117);
            this.AddressReplace.Name = "AddressReplace";
            this.AddressReplace.Size = new System.Drawing.Size(189, 20);
            this.AddressReplace.TabIndex = 9;
            // 
            // button1
            // 
            this.button1.Font = new System.Drawing.Font("Microsoft Sans Serif", 20.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(161)));
            this.button1.Location = new System.Drawing.Point(123, 143);
            this.button1.Name = "button1";
            this.button1.Size = new System.Drawing.Size(183, 59);
            this.button1.TabIndex = 10;
            this.button1.Text = "Αποθήκευση";
            this.button1.UseVisualStyleBackColor = true;
            this.button1.Click += new System.EventHandler(this.button1_Click);
            // 
            // UpdateForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(432, 209);
            this.Controls.Add(this.button1);
            this.Controls.Add(this.AddressReplace);
            this.Controls.Add(this.TelephoneReplace);
            this.Controls.Add(this.LastNameReplace);
            this.Controls.Add(this.FirstNameReplace);
            this.Controls.Add(this.IDReplace);
            this.Controls.Add(this.AddressExist);
            this.Controls.Add(this.TelephoneExist);
            this.Controls.Add(this.LastNameExist);
            this.Controls.Add(this.FirstNameExist);
            this.Controls.Add(this.IDExist);
            this.Name = "UpdateForm";
            this.Text = "UpdateForm";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox IDExist;
        private System.Windows.Forms.TextBox FirstNameExist;
        private System.Windows.Forms.TextBox LastNameExist;
        private System.Windows.Forms.TextBox TelephoneExist;
        private System.Windows.Forms.TextBox AddressExist;
        private System.Windows.Forms.TextBox IDReplace;
        private System.Windows.Forms.TextBox FirstNameReplace;
        private System.Windows.Forms.TextBox LastNameReplace;
        private System.Windows.Forms.TextBox TelephoneReplace;
        private System.Windows.Forms.TextBox AddressReplace;
        private System.Windows.Forms.Button button1;
    }
}
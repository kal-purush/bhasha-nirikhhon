namespace RAD_Project
{
    partial class UserControl1_PInfo
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
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle1 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle2 = new System.Windows.Forms.DataGridViewCellStyle();
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(UserControl1_PInfo));
            this.dataGridView1 = new System.Windows.Forms.DataGridView();
            this.label1 = new System.Windows.Forms.Label();
            this.btnISearch = new System.Windows.Forms.Button();
            this.btnIUpdate = new System.Windows.Forms.Button();
            this.btnIDelete = new System.Windows.Forms.Button();
            this.btnIClose = new System.Windows.Forms.Button();
            this.panel1 = new System.Windows.Forms.Panel();
            this.textBox1 = new System.Windows.Forms.TextBox();
            this.label12 = new System.Windows.Forms.Label();
            this.checkBoxPlaster = new System.Windows.Forms.CheckBox();
            this.checkBoxDrug = new System.Windows.Forms.CheckBox();
            this.checkBoxFood = new System.Windows.Forms.CheckBox();
            this.radioFemale = new System.Windows.Forms.RadioButton();
            this.radioMale = new System.Windows.Forms.RadioButton();
            this.txtContact = new System.Windows.Forms.TextBox();
            this.txtEmergency = new System.Windows.Forms.TextBox();
            this.txtAddress = new System.Windows.Forms.TextBox();
            this.txtBlood = new System.Windows.Forms.TextBox();
            this.txtDOB = new System.Windows.Forms.TextBox();
            this.txtAge = new System.Windows.Forms.TextBox();
            this.txtName = new System.Windows.Forms.TextBox();
            this.txtPID = new System.Windows.Forms.TextBox();
            this.label11 = new System.Windows.Forms.Label();
            this.label10 = new System.Windows.Forms.Label();
            this.label9 = new System.Windows.Forms.Label();
            this.label8 = new System.Windows.Forms.Label();
            this.label7 = new System.Windows.Forms.Label();
            this.label6 = new System.Windows.Forms.Label();
            this.label5 = new System.Windows.Forms.Label();
            this.label4 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            ((System.ComponentModel.ISupportInitialize)(this.dataGridView1)).BeginInit();
            this.panel1.SuspendLayout();
            this.SuspendLayout();
            // 
            // dataGridView1
            // 
            this.dataGridView1.BorderStyle = System.Windows.Forms.BorderStyle.Fixed3D;
            dataGridViewCellStyle1.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleLeft;
            dataGridViewCellStyle1.BackColor = System.Drawing.Color.DeepSkyBlue;
            dataGridViewCellStyle1.Font = new System.Drawing.Font("Microsoft Sans Serif", 7.8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            dataGridViewCellStyle1.ForeColor = System.Drawing.SystemColors.WindowText;
            dataGridViewCellStyle1.SelectionBackColor = System.Drawing.SystemColors.Highlight;
            dataGridViewCellStyle1.SelectionForeColor = System.Drawing.SystemColors.HighlightText;
            dataGridViewCellStyle1.WrapMode = System.Windows.Forms.DataGridViewTriState.True;
            this.dataGridView1.ColumnHeadersDefaultCellStyle = dataGridViewCellStyle1;
            this.dataGridView1.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dataGridView1.Location = new System.Drawing.Point(8, 312);
            this.dataGridView1.Name = "dataGridView1";
            dataGridViewCellStyle2.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleLeft;
            dataGridViewCellStyle2.BackColor = System.Drawing.Color.DeepSkyBlue;
            dataGridViewCellStyle2.Font = new System.Drawing.Font("Microsoft Sans Serif", 7.8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            dataGridViewCellStyle2.ForeColor = System.Drawing.SystemColors.WindowText;
            dataGridViewCellStyle2.SelectionBackColor = System.Drawing.SystemColors.Highlight;
            dataGridViewCellStyle2.SelectionForeColor = System.Drawing.SystemColors.HighlightText;
            dataGridViewCellStyle2.WrapMode = System.Windows.Forms.DataGridViewTriState.True;
            this.dataGridView1.RowHeadersDefaultCellStyle = dataGridViewCellStyle2;
            this.dataGridView1.RowHeadersWidth = 51;
            this.dataGridView1.RowTemplate.Height = 24;
            this.dataGridView1.Size = new System.Drawing.Size(816, 192);
            this.dataGridView1.TabIndex = 0;
            this.dataGridView1.CellContentClick += new System.Windows.Forms.DataGridViewCellEventHandler(this.DataGridView1_CellContentClick);
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Underline))), System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.Location = new System.Drawing.Point(320, 288);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(192, 25);
            this.label1.TabIndex = 1;
            this.label1.Text = "Patient Information";
            // 
            // btnISearch
            // 
            this.btnISearch.BackColor = System.Drawing.SystemColors.ActiveCaptionText;
            this.btnISearch.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnISearch.ForeColor = System.Drawing.SystemColors.Window;
            this.btnISearch.Location = new System.Drawing.Point(400, 224);
            this.btnISearch.Name = "btnISearch";
            this.btnISearch.Size = new System.Drawing.Size(96, 40);
            this.btnISearch.TabIndex = 2;
            this.btnISearch.Text = "Reset";
            this.btnISearch.UseVisualStyleBackColor = false;
            this.btnISearch.Click += new System.EventHandler(this.Button1_Click);
            // 
            // btnIUpdate
            // 
            this.btnIUpdate.BackColor = System.Drawing.SystemColors.ActiveCaptionText;
            this.btnIUpdate.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnIUpdate.ForeColor = System.Drawing.SystemColors.Window;
            this.btnIUpdate.Location = new System.Drawing.Point(504, 224);
            this.btnIUpdate.Name = "btnIUpdate";
            this.btnIUpdate.Size = new System.Drawing.Size(96, 40);
            this.btnIUpdate.TabIndex = 3;
            this.btnIUpdate.Text = "Update";
            this.btnIUpdate.UseVisualStyleBackColor = false;
            this.btnIUpdate.Click += new System.EventHandler(this.BtnIUpdate_Click);
            // 
            // btnIDelete
            // 
            this.btnIDelete.BackColor = System.Drawing.SystemColors.ActiveCaptionText;
            this.btnIDelete.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnIDelete.ForeColor = System.Drawing.SystemColors.Window;
            this.btnIDelete.Location = new System.Drawing.Point(608, 224);
            this.btnIDelete.Name = "btnIDelete";
            this.btnIDelete.Size = new System.Drawing.Size(96, 40);
            this.btnIDelete.TabIndex = 4;
            this.btnIDelete.Text = "Delete";
            this.btnIDelete.UseVisualStyleBackColor = false;
            this.btnIDelete.Click += new System.EventHandler(this.BtnIDelete_Click);
            // 
            // btnIClose
            // 
            this.btnIClose.BackColor = System.Drawing.SystemColors.ActiveCaptionText;
            this.btnIClose.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnIClose.ForeColor = System.Drawing.SystemColors.Window;
            this.btnIClose.Location = new System.Drawing.Point(712, 224);
            this.btnIClose.Name = "btnIClose";
            this.btnIClose.Size = new System.Drawing.Size(96, 40);
            this.btnIClose.TabIndex = 5;
            this.btnIClose.Text = "Close";
            this.btnIClose.UseVisualStyleBackColor = false;
            this.btnIClose.Click += new System.EventHandler(this.Button4_Click);
            // 
            // panel1
            // 
            this.panel1.BackColor = System.Drawing.Color.Transparent;
            this.panel1.Controls.Add(this.btnIClose);
            this.panel1.Controls.Add(this.textBox1);
            this.panel1.Controls.Add(this.label12);
            this.panel1.Controls.Add(this.btnIDelete);
            this.panel1.Controls.Add(this.checkBoxPlaster);
            this.panel1.Controls.Add(this.checkBoxDrug);
            this.panel1.Controls.Add(this.btnIUpdate);
            this.panel1.Controls.Add(this.checkBoxFood);
            this.panel1.Controls.Add(this.radioFemale);
            this.panel1.Controls.Add(this.btnISearch);
            this.panel1.Controls.Add(this.radioMale);
            this.panel1.Controls.Add(this.txtContact);
            this.panel1.Controls.Add(this.txtEmergency);
            this.panel1.Controls.Add(this.txtAddress);
            this.panel1.Controls.Add(this.txtBlood);
            this.panel1.Controls.Add(this.txtDOB);
            this.panel1.Controls.Add(this.txtAge);
            this.panel1.Controls.Add(this.txtName);
            this.panel1.Controls.Add(this.txtPID);
            this.panel1.Controls.Add(this.label11);
            this.panel1.Controls.Add(this.label10);
            this.panel1.Controls.Add(this.label9);
            this.panel1.Controls.Add(this.label8);
            this.panel1.Controls.Add(this.label7);
            this.panel1.Controls.Add(this.label6);
            this.panel1.Controls.Add(this.label5);
            this.panel1.Controls.Add(this.label4);
            this.panel1.Controls.Add(this.label3);
            this.panel1.Controls.Add(this.label2);
            this.panel1.Location = new System.Drawing.Point(8, 8);
            this.panel1.Name = "panel1";
            this.panel1.Size = new System.Drawing.Size(816, 280);
            this.panel1.TabIndex = 10;
            // 
            // textBox1
            // 
            this.textBox1.Location = new System.Drawing.Point(160, 208);
            this.textBox1.Name = "textBox1";
            this.textBox1.Size = new System.Drawing.Size(184, 22);
            this.textBox1.TabIndex = 81;
            // 
            // label12
            // 
            this.label12.AutoSize = true;
            this.label12.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label12.Location = new System.Drawing.Point(16, 208);
            this.label12.Name = "label12";
            this.label12.Size = new System.Drawing.Size(85, 25);
            this.label12.TabIndex = 80;
            this.label12.Text = "Address";
            // 
            // checkBoxPlaster
            // 
            this.checkBoxPlaster.AutoSize = true;
            this.checkBoxPlaster.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.checkBoxPlaster.Location = new System.Drawing.Point(616, 104);
            this.checkBoxPlaster.Name = "checkBoxPlaster";
            this.checkBoxPlaster.Size = new System.Drawing.Size(159, 29);
            this.checkBoxPlaster.TabIndex = 79;
            this.checkBoxPlaster.Text = "Plaster Allergy";
            this.checkBoxPlaster.UseVisualStyleBackColor = true;
            // 
            // checkBoxDrug
            // 
            this.checkBoxDrug.AutoSize = true;
            this.checkBoxDrug.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.checkBoxDrug.Location = new System.Drawing.Point(616, 72);
            this.checkBoxDrug.Name = "checkBoxDrug";
            this.checkBoxDrug.Size = new System.Drawing.Size(141, 29);
            this.checkBoxDrug.TabIndex = 78;
            this.checkBoxDrug.Text = "Drug Allergy";
            this.checkBoxDrug.UseVisualStyleBackColor = true;
            // 
            // checkBoxFood
            // 
            this.checkBoxFood.AutoSize = true;
            this.checkBoxFood.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.checkBoxFood.Location = new System.Drawing.Point(616, 40);
            this.checkBoxFood.Name = "checkBoxFood";
            this.checkBoxFood.Size = new System.Drawing.Size(144, 29);
            this.checkBoxFood.TabIndex = 77;
            this.checkBoxFood.Text = "Food Allergy";
            this.checkBoxFood.UseVisualStyleBackColor = true;
            // 
            // radioFemale
            // 
            this.radioFemale.AutoSize = true;
            this.radioFemale.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.radioFemale.Location = new System.Drawing.Point(248, 88);
            this.radioFemale.Name = "radioFemale";
            this.radioFemale.Size = new System.Drawing.Size(98, 29);
            this.radioFemale.TabIndex = 76;
            this.radioFemale.TabStop = true;
            this.radioFemale.Text = "Female";
            this.radioFemale.UseVisualStyleBackColor = true;
            // 
            // radioMale
            // 
            this.radioMale.AutoSize = true;
            this.radioMale.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.radioMale.Location = new System.Drawing.Point(160, 88);
            this.radioMale.Name = "radioMale";
            this.radioMale.Size = new System.Drawing.Size(76, 29);
            this.radioMale.TabIndex = 75;
            this.radioMale.TabStop = true;
            this.radioMale.Text = "Male";
            this.radioMale.UseVisualStyleBackColor = true;
            // 
            // txtContact
            // 
            this.txtContact.Location = new System.Drawing.Point(616, 8);
            this.txtContact.Name = "txtContact";
            this.txtContact.Size = new System.Drawing.Size(176, 22);
            this.txtContact.TabIndex = 74;
            this.txtContact.TextChanged += new System.EventHandler(this.TxtContact_TextChanged);
            // 
            // txtEmergency
            // 
            this.txtEmergency.Location = new System.Drawing.Point(616, 184);
            this.txtEmergency.Name = "txtEmergency";
            this.txtEmergency.Size = new System.Drawing.Size(176, 22);
            this.txtEmergency.TabIndex = 73;
            // 
            // txtAddress
            // 
            this.txtAddress.Location = new System.Drawing.Point(160, 288);
            this.txtAddress.Name = "txtAddress";
            this.txtAddress.Size = new System.Drawing.Size(632, 22);
            this.txtAddress.TabIndex = 72;
            // 
            // txtBlood
            // 
            this.txtBlood.Location = new System.Drawing.Point(616, 144);
            this.txtBlood.Name = "txtBlood";
            this.txtBlood.Size = new System.Drawing.Size(176, 22);
            this.txtBlood.TabIndex = 71;
            // 
            // txtDOB
            // 
            this.txtDOB.Location = new System.Drawing.Point(160, 168);
            this.txtDOB.Name = "txtDOB";
            this.txtDOB.Size = new System.Drawing.Size(184, 22);
            this.txtDOB.TabIndex = 70;
            // 
            // txtAge
            // 
            this.txtAge.Location = new System.Drawing.Point(160, 136);
            this.txtAge.Name = "txtAge";
            this.txtAge.Size = new System.Drawing.Size(184, 22);
            this.txtAge.TabIndex = 69;
            // 
            // txtName
            // 
            this.txtName.Location = new System.Drawing.Point(160, 48);
            this.txtName.Name = "txtName";
            this.txtName.Size = new System.Drawing.Size(184, 22);
            this.txtName.TabIndex = 68;
            // 
            // txtPID
            // 
            this.txtPID.Location = new System.Drawing.Point(160, 8);
            this.txtPID.Name = "txtPID";
            this.txtPID.Size = new System.Drawing.Size(184, 22);
            this.txtPID.TabIndex = 67;
            this.txtPID.TextChanged += new System.EventHandler(this.TxtPID_TextChanged);
            // 
            // label11
            // 
            this.label11.AutoSize = true;
            this.label11.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label11.Location = new System.Drawing.Point(408, 184);
            this.label11.Name = "label11";
            this.label11.Size = new System.Drawing.Size(184, 25);
            this.label11.TabIndex = 66;
            this.label11.Text = "Emergency Contact";
            // 
            // label10
            // 
            this.label10.AutoSize = true;
            this.label10.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label10.Location = new System.Drawing.Point(408, 144);
            this.label10.Name = "label10";
            this.label10.Size = new System.Drawing.Size(121, 25);
            this.label10.TabIndex = 65;
            this.label10.Text = "Blood Group";
            // 
            // label9
            // 
            this.label9.AutoSize = true;
            this.label9.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label9.Location = new System.Drawing.Point(408, 48);
            this.label9.Name = "label9";
            this.label9.Size = new System.Drawing.Size(87, 25);
            this.label9.TabIndex = 64;
            this.label9.Text = "Allergies";
            // 
            // label8
            // 
            this.label8.AutoSize = true;
            this.label8.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label8.Location = new System.Drawing.Point(408, 8);
            this.label8.Name = "label8";
            this.label8.Size = new System.Drawing.Size(110, 25);
            this.label8.TabIndex = 63;
            this.label8.Text = "Contact No";
            // 
            // label7
            // 
            this.label7.AutoSize = true;
            this.label7.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label7.Location = new System.Drawing.Point(16, 288);
            this.label7.Name = "label7";
            this.label7.Size = new System.Drawing.Size(85, 25);
            this.label7.TabIndex = 62;
            this.label7.Text = "Address";
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label6.Location = new System.Drawing.Point(16, 168);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(123, 25);
            this.label6.TabIndex = 61;
            this.label6.Text = "Date Of Birth";
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label5.Location = new System.Drawing.Point(16, 128);
            this.label5.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(48, 25);
            this.label5.TabIndex = 60;
            this.label5.Text = "Age";
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label4.Location = new System.Drawing.Point(16, 88);
            this.label4.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(77, 25);
            this.label4.TabIndex = 59;
            this.label4.Text = "Gender";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label3.Location = new System.Drawing.Point(16, 48);
            this.label3.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(64, 25);
            this.label3.TabIndex = 58;
            this.label3.Text = "Name";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label2.Location = new System.Drawing.Point(16, 8);
            this.label2.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(96, 25);
            this.label2.TabIndex = 57;
            this.label2.Text = "Patient ID";
            // 
            // UserControl1_PInfo
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.Color.Transparent;
            this.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("$this.BackgroundImage")));
            this.Controls.Add(this.panel1);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.dataGridView1);
            this.Name = "UserControl1_PInfo";
            this.Size = new System.Drawing.Size(832, 507);
            this.Load += new System.EventHandler(this.UserControl1_PInfo_Load);
            ((System.ComponentModel.ISupportInitialize)(this.dataGridView1)).EndInit();
            this.panel1.ResumeLayout(false);
            this.panel1.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.DataGridView dataGridView1;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Button btnISearch;
        private System.Windows.Forms.Button btnIUpdate;
        private System.Windows.Forms.Button btnIDelete;
        private System.Windows.Forms.Button btnIClose;
        private System.Windows.Forms.Panel panel1;
        private System.Windows.Forms.CheckBox checkBoxPlaster;
        private System.Windows.Forms.CheckBox checkBoxDrug;
        private System.Windows.Forms.CheckBox checkBoxFood;
        private System.Windows.Forms.RadioButton radioFemale;
        private System.Windows.Forms.RadioButton radioMale;
        private System.Windows.Forms.TextBox txtContact;
        private System.Windows.Forms.TextBox txtEmergency;
        private System.Windows.Forms.TextBox txtAddress;
        private System.Windows.Forms.TextBox txtBlood;
        private System.Windows.Forms.TextBox txtDOB;
        private System.Windows.Forms.TextBox txtAge;
        private System.Windows.Forms.TextBox txtName;
        private System.Windows.Forms.TextBox txtPID;
        private System.Windows.Forms.Label label11;
        private System.Windows.Forms.Label label10;
        private System.Windows.Forms.Label label9;
        private System.Windows.Forms.Label label8;
        private System.Windows.Forms.Label label7;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.TextBox textBox1;
        private System.Windows.Forms.Label label12;
    }
}
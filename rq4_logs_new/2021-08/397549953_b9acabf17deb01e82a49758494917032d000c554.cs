namespace databaseConnectionExample
{
    partial class Form1
    {
        /// <summary>
        ///Gerekli tasarımcı değişkeni.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        ///Kullanılan tüm kaynakları temizleyin.
        /// </summary>
        ///<param name="disposing">yönetilen kaynaklar dispose edilmeliyse doğru; aksi halde yanlış.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer üretilen kod

        /// <summary>
        /// Tasarımcı desteği için gerekli metot - bu metodun 
        ///içeriğini kod düzenleyici ile değiştirmeyin.
        /// </summary>
        private void InitializeComponent()
        {
            this.label1 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.button1 = new System.Windows.Forms.Button();
            this.comboBox1_customerID = new System.Windows.Forms.ComboBox();
            this.comboBox_EmployeeID = new System.Windows.Forms.ComboBox();
            this.order_date = new System.Windows.Forms.DateTimePicker();
            this.required_date = new System.Windows.Forms.DateTimePicker();
            this.label3 = new System.Windows.Forms.Label();
            this.label4 = new System.Windows.Forms.Label();
            this.textBox_freight = new System.Windows.Forms.TextBox();
            this.label5 = new System.Windows.Forms.Label();
            this.textBox_shipName = new System.Windows.Forms.TextBox();
            this.textBox_shipAdress = new System.Windows.Forms.TextBox();
            this.label6 = new System.Windows.Forms.Label();
            this.label7 = new System.Windows.Forms.Label();
            this.textBox_shipCity = new System.Windows.Forms.TextBox();
            this.textBox_shipCountry = new System.Windows.Forms.TextBox();
            this.label8 = new System.Windows.Forms.Label();
            this.label9 = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(60, 44);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(66, 13);
            this.label1.TabIndex = 0;
            this.label1.Text = "Customer Id:";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(58, 86);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(68, 13);
            this.label2.TabIndex = 1;
            this.label2.Text = "Employee Id:";
            // 
            // button1
            // 
            this.button1.Location = new System.Drawing.Point(159, 332);
            this.button1.Name = "button1";
            this.button1.Size = new System.Drawing.Size(75, 23);
            this.button1.TabIndex = 4;
            this.button1.Text = "Order";
            this.button1.UseVisualStyleBackColor = true;
            this.button1.Click += new System.EventHandler(this.button1_Click);
            // 
            // comboBox1_customerID
            // 
            this.comboBox1_customerID.FormattingEnabled = true;
            this.comboBox1_customerID.Location = new System.Drawing.Point(148, 36);
            this.comboBox1_customerID.Name = "comboBox1_customerID";
            this.comboBox1_customerID.Size = new System.Drawing.Size(121, 21);
            this.comboBox1_customerID.TabIndex = 5;
            // 
            // comboBox_EmployeeID
            // 
            this.comboBox_EmployeeID.FormattingEnabled = true;
            this.comboBox_EmployeeID.Location = new System.Drawing.Point(148, 78);
            this.comboBox_EmployeeID.Name = "comboBox_EmployeeID";
            this.comboBox_EmployeeID.Size = new System.Drawing.Size(121, 21);
            this.comboBox_EmployeeID.TabIndex = 6;
            // 
            // order_date
            // 
            this.order_date.Location = new System.Drawing.Point(390, 36);
            this.order_date.Name = "order_date";
            this.order_date.Size = new System.Drawing.Size(200, 20);
            this.order_date.TabIndex = 7;
            // 
            // required_date
            // 
            this.required_date.Location = new System.Drawing.Point(390, 75);
            this.required_date.Name = "required_date";
            this.required_date.Size = new System.Drawing.Size(200, 20);
            this.required_date.TabIndex = 8;
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(322, 42);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(62, 13);
            this.label3.TabIndex = 10;
            this.label3.Text = "Order Date:";
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(308, 82);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(79, 13);
            this.label4.TabIndex = 11;
            this.label4.Text = "Required Date:";
            // 
            // textBox_freight
            // 
            this.textBox_freight.Location = new System.Drawing.Point(148, 124);
            this.textBox_freight.Name = "textBox_freight";
            this.textBox_freight.Size = new System.Drawing.Size(121, 20);
            this.textBox_freight.TabIndex = 12;
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Location = new System.Drawing.Point(84, 127);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(42, 13);
            this.label5.TabIndex = 13;
            this.label5.Text = "Freight:";
            // 
            // textBox_shipName
            // 
            this.textBox_shipName.Location = new System.Drawing.Point(148, 165);
            this.textBox_shipName.Name = "textBox_shipName";
            this.textBox_shipName.Size = new System.Drawing.Size(121, 20);
            this.textBox_shipName.TabIndex = 14;
            // 
            // textBox_shipAdress
            // 
            this.textBox_shipAdress.Location = new System.Drawing.Point(148, 204);
            this.textBox_shipAdress.Name = "textBox_shipAdress";
            this.textBox_shipAdress.Size = new System.Drawing.Size(121, 20);
            this.textBox_shipAdress.TabIndex = 15;
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Location = new System.Drawing.Point(64, 172);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(62, 13);
            this.label6.TabIndex = 16;
            this.label6.Text = "Ship Name:";
            // 
            // label7
            // 
            this.label7.AutoSize = true;
            this.label7.Location = new System.Drawing.Point(60, 211);
            this.label7.Name = "label7";
            this.label7.Size = new System.Drawing.Size(66, 13);
            this.label7.TabIndex = 17;
            this.label7.Text = "Ship Adress:";
            // 
            // textBox_shipCity
            // 
            this.textBox_shipCity.Location = new System.Drawing.Point(148, 244);
            this.textBox_shipCity.Name = "textBox_shipCity";
            this.textBox_shipCity.Size = new System.Drawing.Size(121, 20);
            this.textBox_shipCity.TabIndex = 18;
            // 
            // textBox_shipCountry
            // 
            this.textBox_shipCountry.Location = new System.Drawing.Point(148, 287);
            this.textBox_shipCountry.Name = "textBox_shipCountry";
            this.textBox_shipCountry.Size = new System.Drawing.Size(121, 20);
            this.textBox_shipCountry.TabIndex = 19;
            // 
            // label8
            // 
            this.label8.AutoSize = true;
            this.label8.Location = new System.Drawing.Point(75, 251);
            this.label8.Name = "label8";
            this.label8.Size = new System.Drawing.Size(51, 13);
            this.label8.TabIndex = 20;
            this.label8.Text = "Ship City:";
            // 
            // label9
            // 
            this.label9.AutoSize = true;
            this.label9.Location = new System.Drawing.Point(56, 294);
            this.label9.Name = "label9";
            this.label9.Size = new System.Drawing.Size(70, 13);
            this.label9.TabIndex = 21;
            this.label9.Text = "Ship Country:";
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(676, 425);
            this.Controls.Add(this.label9);
            this.Controls.Add(this.label8);
            this.Controls.Add(this.textBox_shipCountry);
            this.Controls.Add(this.textBox_shipCity);
            this.Controls.Add(this.label7);
            this.Controls.Add(this.label6);
            this.Controls.Add(this.textBox_shipAdress);
            this.Controls.Add(this.textBox_shipName);
            this.Controls.Add(this.label5);
            this.Controls.Add(this.textBox_freight);
            this.Controls.Add(this.label4);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.required_date);
            this.Controls.Add(this.order_date);
            this.Controls.Add(this.comboBox_EmployeeID);
            this.Controls.Add(this.comboBox1_customerID);
            this.Controls.Add(this.button1);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.label1);
            this.Name = "Form1";
            this.Text = "Form1";
            this.FormClosed += new System.Windows.Forms.FormClosedEventHandler(this.Form1_FormClosed);
            this.Load += new System.EventHandler(this.Form1_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Button button1;
        private System.Windows.Forms.ComboBox comboBox1_customerID;
        private System.Windows.Forms.ComboBox comboBox_EmployeeID;
        private System.Windows.Forms.DateTimePicker order_date;
        private System.Windows.Forms.DateTimePicker required_date;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.TextBox textBox_freight;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.TextBox textBox_shipName;
        private System.Windows.Forms.TextBox textBox_shipAdress;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.Label label7;
        private System.Windows.Forms.TextBox textBox_shipCity;
        private System.Windows.Forms.TextBox textBox_shipCountry;
        private System.Windows.Forms.Label label8;
        private System.Windows.Forms.Label label9;
    }
}

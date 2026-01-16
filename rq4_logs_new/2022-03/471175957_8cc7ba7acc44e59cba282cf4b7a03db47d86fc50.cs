
namespace EF_Project.Forms
{
    partial class Moving
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(Moving));
            this.ExitWareHouse = new System.Windows.Forms.PictureBox();
            this.Back = new System.Windows.Forms.PictureBox();
            this.groupBox2 = new System.Windows.Forms.GroupBox();
            this.label1 = new System.Windows.Forms.Label();
            this.TocomboBox1 = new System.Windows.Forms.ComboBox();
            this.label6 = new System.Windows.Forms.Label();
            this.FromcomboBox = new System.Windows.Forms.ComboBox();
            this.groupBox3 = new System.Windows.Forms.GroupBox();
            this.TransactionButton = new System.Windows.Forms.Button();
            this.QuantitytextBox = new System.Windows.Forms.TextBox();
            this.ProductIDlabel1 = new System.Windows.Forms.Label();
            this.label4 = new System.Windows.Forms.Label();
            this.ItemcomboBox3 = new System.Windows.Forms.ComboBox();
            this.label2 = new System.Windows.Forms.Label();
            this.ExpireDurcomboBox1 = new System.Windows.Forms.ComboBox();
            this.label3 = new System.Windows.Forms.Label();
            this.ProdDatecomboBox2 = new System.Windows.Forms.ComboBox();
            this.label5 = new System.Windows.Forms.Label();
            this.SuppliercomboBox4 = new System.Windows.Forms.ComboBox();
            ((System.ComponentModel.ISupportInitialize)(this.ExitWareHouse)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.Back)).BeginInit();
            this.groupBox2.SuspendLayout();
            this.groupBox3.SuspendLayout();
            this.SuspendLayout();
            // 
            // ExitWareHouse
            // 
            this.ExitWareHouse.Image = ((System.Drawing.Image)(resources.GetObject("ExitWareHouse.Image")));
            this.ExitWareHouse.Location = new System.Drawing.Point(700, 12);
            this.ExitWareHouse.Name = "ExitWareHouse";
            this.ExitWareHouse.Size = new System.Drawing.Size(86, 81);
            this.ExitWareHouse.SizeMode = System.Windows.Forms.PictureBoxSizeMode.StretchImage;
            this.ExitWareHouse.TabIndex = 9;
            this.ExitWareHouse.TabStop = false;
            this.ExitWareHouse.Click += new System.EventHandler(this.ExitWareHouse_Click);
            // 
            // Back
            // 
            this.Back.Image = ((System.Drawing.Image)(resources.GetObject("Back.Image")));
            this.Back.Location = new System.Drawing.Point(12, 12);
            this.Back.Name = "Back";
            this.Back.Size = new System.Drawing.Size(86, 81);
            this.Back.SizeMode = System.Windows.Forms.PictureBoxSizeMode.StretchImage;
            this.Back.TabIndex = 10;
            this.Back.TabStop = false;
            this.Back.Click += new System.EventHandler(this.Back_Click);
            // 
            // groupBox2
            // 
            this.groupBox2.Controls.Add(this.label1);
            this.groupBox2.Controls.Add(this.TocomboBox1);
            this.groupBox2.Controls.Add(this.label6);
            this.groupBox2.Controls.Add(this.FromcomboBox);
            this.groupBox2.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.groupBox2.ForeColor = System.Drawing.Color.LightSeaGreen;
            this.groupBox2.Location = new System.Drawing.Point(23, 124);
            this.groupBox2.Name = "groupBox2";
            this.groupBox2.Size = new System.Drawing.Size(755, 151);
            this.groupBox2.TabIndex = 30;
            this.groupBox2.TabStop = false;
            this.groupBox2.Text = "WareHouses";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.ForeColor = System.Drawing.Color.Black;
            this.label1.Location = new System.Drawing.Point(27, 101);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(38, 25);
            this.label1.TabIndex = 33;
            this.label1.Text = "To";
            // 
            // TocomboBox1
            // 
            this.TocomboBox1.FormattingEnabled = true;
            this.TocomboBox1.Location = new System.Drawing.Point(104, 99);
            this.TocomboBox1.Name = "TocomboBox1";
            this.TocomboBox1.Size = new System.Drawing.Size(606, 33);
            this.TocomboBox1.TabIndex = 32;
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label6.ForeColor = System.Drawing.Color.Black;
            this.label6.Location = new System.Drawing.Point(27, 41);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(61, 25);
            this.label6.TabIndex = 31;
            this.label6.Text = "From";
            // 
            // FromcomboBox
            // 
            this.FromcomboBox.FormattingEnabled = true;
            this.FromcomboBox.Location = new System.Drawing.Point(104, 39);
            this.FromcomboBox.Name = "FromcomboBox";
            this.FromcomboBox.Size = new System.Drawing.Size(606, 33);
            this.FromcomboBox.TabIndex = 30;
            this.FromcomboBox.SelectedIndexChanged += new System.EventHandler(this.FromcomboBox_SelectedIndexChanged);
            // 
            // groupBox3
            // 
            this.groupBox3.Controls.Add(this.label5);
            this.groupBox3.Controls.Add(this.SuppliercomboBox4);
            this.groupBox3.Controls.Add(this.TransactionButton);
            this.groupBox3.Controls.Add(this.QuantitytextBox);
            this.groupBox3.Controls.Add(this.ProductIDlabel1);
            this.groupBox3.Controls.Add(this.label4);
            this.groupBox3.Controls.Add(this.ItemcomboBox3);
            this.groupBox3.Controls.Add(this.label2);
            this.groupBox3.Controls.Add(this.ExpireDurcomboBox1);
            this.groupBox3.Controls.Add(this.label3);
            this.groupBox3.Controls.Add(this.ProdDatecomboBox2);
            this.groupBox3.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.groupBox3.ForeColor = System.Drawing.Color.LightSeaGreen;
            this.groupBox3.Location = new System.Drawing.Point(20, 281);
            this.groupBox3.Name = "groupBox3";
            this.groupBox3.Size = new System.Drawing.Size(758, 311);
            this.groupBox3.TabIndex = 31;
            this.groupBox3.TabStop = false;
            this.groupBox3.Text = "Item Options";
            // 
            // TransactionButton
            // 
            this.TransactionButton.BackColor = System.Drawing.Color.LightSeaGreen;
            this.TransactionButton.ForeColor = System.Drawing.Color.White;
            this.TransactionButton.Location = new System.Drawing.Point(434, 251);
            this.TransactionButton.Name = "TransactionButton";
            this.TransactionButton.Size = new System.Drawing.Size(305, 50);
            this.TransactionButton.TabIndex = 40;
            this.TransactionButton.Text = "Transact";
            this.TransactionButton.UseVisualStyleBackColor = false;
            this.TransactionButton.Click += new System.EventHandler(this.TransactionButton_Click);
            // 
            // QuantitytextBox
            // 
            this.QuantitytextBox.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.QuantitytextBox.Location = new System.Drawing.Point(221, 260);
            this.QuantitytextBox.Multiline = true;
            this.QuantitytextBox.Name = "QuantitytextBox";
            this.QuantitytextBox.Size = new System.Drawing.Size(160, 28);
            this.QuantitytextBox.TabIndex = 39;
            // 
            // ProductIDlabel1
            // 
            this.ProductIDlabel1.AutoSize = true;
            this.ProductIDlabel1.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.ProductIDlabel1.ForeColor = System.Drawing.Color.Black;
            this.ProductIDlabel1.Location = new System.Drawing.Point(43, 263);
            this.ProductIDlabel1.Name = "ProductIDlabel1";
            this.ProductIDlabel1.Size = new System.Drawing.Size(93, 25);
            this.ProductIDlabel1.TabIndex = 38;
            this.ProductIDlabel1.Text = "Quantity";
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label4.ForeColor = System.Drawing.Color.Black;
            this.label4.Location = new System.Drawing.Point(38, 45);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(53, 25);
            this.label4.TabIndex = 35;
            this.label4.Text = "Item";
            // 
            // ItemcomboBox3
            // 
            this.ItemcomboBox3.FormattingEnabled = true;
            this.ItemcomboBox3.Location = new System.Drawing.Point(221, 37);
            this.ItemcomboBox3.Name = "ItemcomboBox3";
            this.ItemcomboBox3.Size = new System.Drawing.Size(485, 33);
            this.ItemcomboBox3.TabIndex = 34;
            this.ItemcomboBox3.SelectedIndexChanged += new System.EventHandler(this.ItemcomboBox3_SelectedIndexChanged);
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label2.ForeColor = System.Drawing.Color.Black;
            this.label2.Location = new System.Drawing.Point(33, 197);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(149, 25);
            this.label2.TabIndex = 33;
            this.label2.Text = "Epire Duration";
            // 
            // ExpireDurcomboBox1
            // 
            this.ExpireDurcomboBox1.FormattingEnabled = true;
            this.ExpireDurcomboBox1.Location = new System.Drawing.Point(221, 192);
            this.ExpireDurcomboBox1.Name = "ExpireDurcomboBox1";
            this.ExpireDurcomboBox1.Size = new System.Drawing.Size(485, 33);
            this.ExpireDurcomboBox1.TabIndex = 32;
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label3.ForeColor = System.Drawing.Color.Black;
            this.label3.Location = new System.Drawing.Point(30, 140);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(166, 25);
            this.label3.TabIndex = 31;
            this.label3.Text = "Production Date";
            // 
            // ProdDatecomboBox2
            // 
            this.ProdDatecomboBox2.FormattingEnabled = true;
            this.ProdDatecomboBox2.Location = new System.Drawing.Point(221, 137);
            this.ProdDatecomboBox2.Name = "ProdDatecomboBox2";
            this.ProdDatecomboBox2.Size = new System.Drawing.Size(485, 33);
            this.ProdDatecomboBox2.TabIndex = 30;
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label5.ForeColor = System.Drawing.Color.Black;
            this.label5.Location = new System.Drawing.Point(38, 91);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(92, 25);
            this.label5.TabIndex = 42;
            this.label5.Text = "Supplier";
            // 
            // SuppliercomboBox4
            // 
            this.SuppliercomboBox4.FormattingEnabled = true;
            this.SuppliercomboBox4.Location = new System.Drawing.Point(221, 86);
            this.SuppliercomboBox4.Name = "SuppliercomboBox4";
            this.SuppliercomboBox4.Size = new System.Drawing.Size(485, 33);
            this.SuppliercomboBox4.TabIndex = 41;
            // 
            // Moving
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 604);
            this.Controls.Add(this.groupBox3);
            this.Controls.Add(this.groupBox2);
            this.Controls.Add(this.Back);
            this.Controls.Add(this.ExitWareHouse);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.None;
            this.Name = "Moving";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Moving";
            this.Load += new System.EventHandler(this.Moving_Load);
            ((System.ComponentModel.ISupportInitialize)(this.ExitWareHouse)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.Back)).EndInit();
            this.groupBox2.ResumeLayout(false);
            this.groupBox2.PerformLayout();
            this.groupBox3.ResumeLayout(false);
            this.groupBox3.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.PictureBox ExitWareHouse;
        private System.Windows.Forms.PictureBox Back;
        private System.Windows.Forms.GroupBox groupBox2;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.ComboBox TocomboBox1;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.ComboBox FromcomboBox;
        private System.Windows.Forms.GroupBox groupBox3;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.ComboBox ExpireDurcomboBox1;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.ComboBox ProdDatecomboBox2;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.ComboBox ItemcomboBox3;
        private System.Windows.Forms.TextBox QuantitytextBox;
        private System.Windows.Forms.Label ProductIDlabel1;
        private System.Windows.Forms.Button TransactionButton;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.ComboBox SuppliercomboBox4;
    }
}
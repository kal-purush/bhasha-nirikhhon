namespace ISM.WebUI
{
    partial class Storages
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
            button1 = new Button();
            dataGridView1 = new DataGridView();
            StorageId = new DataGridViewTextBoxColumn();
            ItemName = new DataGridViewTextBoxColumn();
            itemDescryption = new DataGridViewTextBoxColumn();
            itemAmount = new DataGridViewTextBoxColumn();
            textBox1 = new TextBox();
            button2 = new Button();
            button3 = new Button();
            button4 = new Button();
            button5 = new Button();
            button6 = new Button();
            vScrollBar1 = new VScrollBar();
            textBox2 = new TextBox();
            textBox3 = new TextBox();
            textBox4 = new TextBox();
            textBox5 = new TextBox();
            label1 = new Label();
            label2 = new Label();
            label3 = new Label();
            label4 = new Label();
            label5 = new Label();
            ((System.ComponentModel.ISupportInitialize)dataGridView1).BeginInit();
            SuspendLayout();
            // 
            // button1
            // 
            button1.Location = new Point(817, 0);
            button1.Name = "button1";
            button1.Size = new Size(36, 27);
            button1.TabIndex = 0;
            button1.Text = "x";
            button1.UseVisualStyleBackColor = true;
            button1.Click += button1_Click;
            // 
            // dataGridView1
            // 
            dataGridView1.ColumnHeadersHeightSizeMode = DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            dataGridView1.Columns.AddRange(new DataGridViewColumn[] { StorageId, ItemName, itemDescryption, itemAmount });
            dataGridView1.Location = new Point(256, 88);
            dataGridView1.Name = "dataGridView1";
            dataGridView1.RowHeadersWidth = 51;
            dataGridView1.RowTemplate.Height = 29;
            dataGridView1.Size = new Size(553, 305);
            dataGridView1.TabIndex = 1;
            // 
            // StorageId
            // 
            StorageId.HeaderText = "Id";
            StorageId.MinimumWidth = 6;
            StorageId.Name = "StorageId";
            StorageId.Width = 125;
            // 
            // ItemName
            // 
            ItemName.HeaderText = "Name";
            ItemName.MinimumWidth = 6;
            ItemName.Name = "ItemName";
            ItemName.Width = 125;
            // 
            // itemDescryption
            // 
            itemDescryption.HeaderText = "Description";
            itemDescryption.MinimumWidth = 6;
            itemDescryption.Name = "itemDescryption";
            itemDescryption.Width = 125;
            // 
            // itemAmount
            // 
            itemAmount.HeaderText = "Amount";
            itemAmount.MinimumWidth = 6;
            itemAmount.Name = "itemAmount";
            itemAmount.Width = 125;
            // 
            // textBox1
            // 
            textBox1.Location = new Point(324, 44);
            textBox1.Name = "textBox1";
            textBox1.Size = new Size(167, 27);
            textBox1.TabIndex = 2;
            // 
            // button2
            // 
            button2.Location = new Point(506, 42);
            button2.Name = "button2";
            button2.Size = new Size(94, 29);
            button2.TabIndex = 3;
            button2.Text = "Search";
            button2.UseVisualStyleBackColor = true;
            // 
            // button3
            // 
            button3.Location = new Point(43, 262);
            button3.Name = "button3";
            button3.Size = new Size(119, 42);
            button3.TabIndex = 4;
            button3.Text = "Add";
            button3.UseVisualStyleBackColor = true;
            button3.Click += button3_Click;
            // 
            // button4
            // 
            button4.Location = new Point(367, 441);
            button4.Name = "button4";
            button4.Size = new Size(124, 44);
            button4.TabIndex = 5;
            button4.Text = "Home";
            button4.UseVisualStyleBackColor = true;
            button4.Click += button4_Click;
            // 
            // button5
            // 
            button5.Location = new Point(43, 310);
            button5.Name = "button5";
            button5.Size = new Size(119, 38);
            button5.TabIndex = 6;
            button5.Text = "Edit";
            button5.UseVisualStyleBackColor = true;
            // 
            // button6
            // 
            button6.Location = new Point(43, 354);
            button6.Name = "button6";
            button6.Size = new Size(119, 39);
            button6.TabIndex = 7;
            button6.Text = "Delete";
            button6.UseVisualStyleBackColor = true;
            // 
            // vScrollBar1
            // 
            vScrollBar1.Location = new Point(803, 88);
            vScrollBar1.Name = "vScrollBar1";
            vScrollBar1.Size = new Size(26, 305);
            vScrollBar1.TabIndex = 8;
            // 
            // textBox2
            // 
            textBox2.Location = new Point(37, 78);
            textBox2.Name = "textBox2";
            textBox2.Size = new Size(125, 27);
            textBox2.TabIndex = 9;
            // 
            // textBox3
            // 
            textBox3.Location = new Point(37, 120);
            textBox3.Name = "textBox3";
            textBox3.Size = new Size(125, 27);
            textBox3.TabIndex = 10;
            // 
            // textBox4
            // 
            textBox4.Location = new Point(37, 164);
            textBox4.Name = "textBox4";
            textBox4.Size = new Size(125, 27);
            textBox4.TabIndex = 11;
            // 
            // textBox5
            // 
            textBox5.Location = new Point(37, 208);
            textBox5.Name = "textBox5";
            textBox5.Size = new Size(125, 27);
            textBox5.TabIndex = 12;
            // 
            // label1
            // 
            label1.AutoSize = true;
            label1.Location = new Point(168, 81);
            label1.Name = "label1";
            label1.Size = new Size(22, 20);
            label1.TabIndex = 13;
            label1.Text = "Id";
            // 
            // label2
            // 
            label2.AutoSize = true;
            label2.Location = new Point(168, 123);
            label2.Name = "label2";
            label2.Size = new Size(49, 20);
            label2.TabIndex = 14;
            label2.Text = "Name";
            // 
            // label3
            // 
            label3.AutoSize = true;
            label3.Location = new Point(168, 167);
            label3.Name = "label3";
            label3.Size = new Size(85, 20);
            label3.TabIndex = 15;
            label3.Text = "Description";
            // 
            // label4
            // 
            label4.AutoSize = true;
            label4.Location = new Point(168, 211);
            label4.Name = "label4";
            label4.Size = new Size(62, 20);
            label4.TabIndex = 16;
            label4.Text = "Amount";
            // 
            // label5
            // 
            label5.AutoSize = true;
            label5.Location = new Point(1, 0);
            label5.Name = "label5";
            label5.Size = new Size(61, 20);
            label5.TabIndex = 17;
            label5.Text = "Storage";
            // 
            // Storage
            // 
            AutoScaleDimensions = new SizeF(8F, 20F);
            AutoScaleMode = AutoScaleMode.Font;
            ClientSize = new Size(853, 497);
            Controls.Add(label5);
            Controls.Add(label4);
            Controls.Add(label3);
            Controls.Add(label2);
            Controls.Add(label1);
            Controls.Add(textBox5);
            Controls.Add(textBox4);
            Controls.Add(textBox3);
            Controls.Add(textBox2);
            Controls.Add(vScrollBar1);
            Controls.Add(button6);
            Controls.Add(button5);
            Controls.Add(button4);
            Controls.Add(button3);
            Controls.Add(button2);
            Controls.Add(textBox1);
            Controls.Add(dataGridView1);
            Controls.Add(button1);
            FormBorderStyle = FormBorderStyle.None;
            Name = "Storage";
            StartPosition = FormStartPosition.CenterScreen;
            Text = "Storage";
            ((System.ComponentModel.ISupportInitialize)dataGridView1).EndInit();
            ResumeLayout(false);
            PerformLayout();
        }

        #endregion

        private Button button1;
        private DataGridView dataGridView1;
        private TextBox textBox1;
        private Button button2;
        private Button button3;
        private Button button4;
        private Button button5;
        private Button button6;
        private DataGridViewTextBoxColumn StorageId;
        private DataGridViewTextBoxColumn ItemName;
        private DataGridViewTextBoxColumn itemDescryption;
        private DataGridViewTextBoxColumn itemAmount;
        private VScrollBar vScrollBar1;
        private TextBox textBox2;
        private TextBox textBox3;
        private TextBox textBox4;
        private TextBox textBox5;
        private Label label1;
        private Label label2;
        private Label label3;
        private Label label4;
        private Label label5;
    }
}
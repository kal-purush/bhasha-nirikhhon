namespace ISM.WebUI
{
    partial class Users
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
            dataGridView1 = new DataGridView();
            dataGridView2 = new DataGridView();
            textBox1 = new TextBox();
            SearchB = new Button();
            idbox = new TextBox();
            Firstnamebox = new TextBox();
            label1 = new Label();
            label2 = new Label();
            button1 = new Button();
            button2 = new Button();
            button3 = new Button();
            label3 = new Label();
            panel1 = new Panel();
            label4 = new Label();
            Lastnamebox = new TextBox();
            label5 = new Label();
            Emailbox = new TextBox();
            passwordbox = new TextBox();
            Email = new Label();
            label6 = new Label();
            ((System.ComponentModel.ISupportInitialize)dataGridView1).BeginInit();
            ((System.ComponentModel.ISupportInitialize)dataGridView2).BeginInit();
            panel1.SuspendLayout();
            SuspendLayout();
            // 
            // dataGridView1
            // 
            dataGridView1.ColumnHeadersHeightSizeMode = DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            dataGridView1.Location = new Point(346, 109);
            dataGridView1.Name = "dataGridView1";
            dataGridView1.RowHeadersWidth = 51;
            dataGridView1.RowTemplate.Height = 29;
            dataGridView1.Size = new Size(419, 217);
            dataGridView1.TabIndex = 0;
            // 
            // dataGridView2
            // 
            dataGridView2.ColumnHeadersHeightSizeMode = DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            dataGridView2.Location = new Point(60, 109);
            dataGridView2.Name = "dataGridView2";
            dataGridView2.RowHeadersWidth = 51;
            dataGridView2.RowTemplate.Height = 29;
            dataGridView2.Size = new Size(187, 217);
            dataGridView2.TabIndex = 1;
            // 
            // textBox1
            // 
            textBox1.Location = new Point(371, 36);
            textBox1.Name = "textBox1";
            textBox1.Size = new Size(125, 27);
            textBox1.TabIndex = 2;
            // 
            // SearchB
            // 
            SearchB.Location = new Point(518, 34);
            SearchB.Name = "SearchB";
            SearchB.Size = new Size(94, 29);
            SearchB.TabIndex = 3;
            SearchB.Text = "Search";
            SearchB.UseVisualStyleBackColor = true;
            // 
            // idbox
            // 
            idbox.Location = new Point(12, 363);
            idbox.Name = "idbox";
            idbox.Size = new Size(125, 27);
            idbox.TabIndex = 4;
            idbox.TextChanged += idbox_TextChanged;
            // 
            // Firstnamebox
            // 
            Firstnamebox.Location = new Point(137, 363);
            Firstnamebox.Name = "Firstnamebox";
            Firstnamebox.Size = new Size(125, 27);
            Firstnamebox.TabIndex = 5;
            // 
            // label1
            // 
            label1.AutoSize = true;
            label1.Location = new Point(60, 338);
            label1.Name = "label1";
            label1.Size = new Size(24, 20);
            label1.TabIndex = 6;
            label1.Text = "ID";
            // 
            // label2
            // 
            label2.AutoSize = true;
            label2.Location = new Point(210, 337);
            label2.Name = "label2";
            label2.Size = new Size(0, 20);
            label2.TabIndex = 7;
            // 
            // button1
            // 
            button1.Location = new Point(627, 328);
            button1.Name = "button1";
            button1.Size = new Size(94, 29);
            button1.TabIndex = 8;
            button1.Text = "Add";
            button1.UseVisualStyleBackColor = true;
            button1.Click += button1_Click;
            // 
            // button2
            // 
            button2.Location = new Point(627, 361);
            button2.Name = "button2";
            button2.Size = new Size(94, 29);
            button2.TabIndex = 9;
            button2.Text = "Delete";
            button2.UseVisualStyleBackColor = true;
            // 
            // button3
            // 
            button3.Location = new Point(627, 396);
            button3.Name = "button3";
            button3.Size = new Size(94, 29);
            button3.TabIndex = 10;
            button3.Text = "Edit";
            button3.UseVisualStyleBackColor = true;
            button3.Click += button3_Click;
            // 
            // label3
            // 
            label3.AutoSize = true;
            label3.Location = new Point(12, 9);
            label3.Name = "label3";
            label3.Size = new Size(44, 20);
            label3.TabIndex = 11;
            label3.Text = "Users";
            // 
            // panel1
            // 
            panel1.BackColor = SystemColors.AppWorkspace;
            panel1.Controls.Add(textBox1);
            panel1.Controls.Add(label3);
            panel1.Controls.Add(SearchB);
            panel1.Location = new Point(0, 0);
            panel1.Name = "panel1";
            panel1.Size = new Size(818, 80);
            panel1.TabIndex = 12;
            // 
            // label4
            // 
            label4.AutoSize = true;
            label4.Location = new Point(174, 338);
            label4.Name = "label4";
            label4.Size = new Size(73, 20);
            label4.TabIndex = 13;
            label4.Text = "Firstname";
            // 
            // Lastnamebox
            // 
            Lastnamebox.Location = new Point(253, 363);
            Lastnamebox.Name = "Lastnamebox";
            Lastnamebox.Size = new Size(125, 27);
            Lastnamebox.TabIndex = 14;
            // 
            // label5
            // 
            label5.AutoSize = true;
            label5.Location = new Point(306, 339);
            label5.Name = "label5";
            label5.Size = new Size(72, 20);
            label5.TabIndex = 15;
            label5.Text = "Lastname";
            // 
            // Emailbox
            // 
            Emailbox.Location = new Point(371, 363);
            Emailbox.Name = "Emailbox";
            Emailbox.Size = new Size(125, 27);
            Emailbox.TabIndex = 16;
            // 
            // passwordbox
            // 
            passwordbox.Location = new Point(496, 363);
            passwordbox.Name = "passwordbox";
            passwordbox.Size = new Size(125, 27);
            passwordbox.TabIndex = 17;
            // 
            // Email
            // 
            Email.AutoSize = true;
            Email.Location = new Point(415, 336);
            Email.Name = "Email";
            Email.Size = new Size(46, 20);
            Email.TabIndex = 18;
            Email.Text = "Email";
            // 
            // label6
            // 
            label6.AutoSize = true;
            label6.Location = new Point(507, 340);
            label6.Name = "label6";
            label6.Size = new Size(70, 20);
            label6.TabIndex = 20;
            label6.Text = "Password";
            // 
            // Users
            // 
            AutoScaleDimensions = new SizeF(8F, 20F);
            AutoScaleMode = AutoScaleMode.Font;
            ClientSize = new Size(800, 450);
            Controls.Add(label6);
            Controls.Add(Email);
            Controls.Add(passwordbox);
            Controls.Add(Emailbox);
            Controls.Add(label5);
            Controls.Add(Lastnamebox);
            Controls.Add(label4);
            Controls.Add(panel1);
            Controls.Add(button3);
            Controls.Add(button2);
            Controls.Add(button1);
            Controls.Add(label2);
            Controls.Add(label1);
            Controls.Add(Firstnamebox);
            Controls.Add(idbox);
            Controls.Add(dataGridView2);
            Controls.Add(dataGridView1);
            FormBorderStyle = FormBorderStyle.None;
            Name = "Users";
            Text = "Users";
            Load += Users_Load;
            ((System.ComponentModel.ISupportInitialize)dataGridView1).EndInit();
            ((System.ComponentModel.ISupportInitialize)dataGridView2).EndInit();
            panel1.ResumeLayout(false);
            panel1.PerformLayout();
            ResumeLayout(false);
            PerformLayout();
        }

        #endregion

        private DataGridView dataGridView1;
        private DataGridView dataGridView2;
        private TextBox textBox1;
        private Button SearchB;
        private TextBox idbox;
        private TextBox Firstnamebox;
        private Label label1;
        private Label label2;
        private Button button1;
        private Button button2;
        private Button button3;
        private Label label3;
        private Panel panel1;
        private Label label4;
        private TextBox Lastnamebox;
        private Label label5;
        private TextBox Emailbox;
        private TextBox passwordbox;
        private Label Email;
        private Label label6;
    }
}
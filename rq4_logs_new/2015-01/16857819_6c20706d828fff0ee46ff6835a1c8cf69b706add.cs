namespace AvitoRuslanParser
{
    partial class frmMain
    {
        /// <summary>
        /// Требуется переменная конструктора.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Освободить все используемые ресурсы.
        /// </summary>
        /// <param name="disposing">истинно, если управляемый ресурс должен быть удален; иначе ложно.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Код, автоматически созданный конструктором форм Windows

        /// <summary>
        /// Обязательный метод для поддержки конструктора - не изменяйте
        /// содержимое данного метода при помощи редактора кода.
        /// </summary>
        private void InitializeComponent()
        {
      this.btnEnter = new System.Windows.Forms.Button();
      this.label1 = new System.Windows.Forms.Label();
      this.LinkAdtextBox = new System.Windows.Forms.TextBox();
      this.label2 = new System.Windows.Forms.Label();
      this.pathToImgtextBox = new System.Windows.Forms.TextBox();
      this.label3 = new System.Windows.Forms.Label();
      this.pathToProxytextBox = new System.Windows.Forms.TextBox();
      this.groupBox1 = new System.Windows.Forms.GroupBox();
      this.textBoxSleep = new System.Windows.Forms.TextBox();
      this.label7 = new System.Windows.Forms.Label();
      this.PasswordtextBox = new System.Windows.Forms.TextBox();
      this.userNametextBox = new System.Windows.Forms.TextBox();
      this.label5 = new System.Windows.Forms.Label();
      this.label4 = new System.Windows.Forms.Label();
      this.label6 = new System.Windows.Forms.Label();
      this.lbCategories = new System.Windows.Forms.ListBox();
      this.btnParsingAvito = new System.Windows.Forms.Button();
      this.label9 = new System.Windows.Forms.Label();
      this.label10 = new System.Windows.Forms.Label();
      this.labelParsed = new System.Windows.Forms.Label();
      this.labelInserted = new System.Windows.Forms.Label();
      this.buttonParsingEbay = new System.Windows.Forms.Button();
      this.buttonParsingAvitoEbay = new System.Windows.Forms.Button();
      this.groupBox1.SuspendLayout();
      this.SuspendLayout();
      // 
      // btnEnter
      // 
      this.btnEnter.Location = new System.Drawing.Point(25, 320);
      this.btnEnter.Name = "btnEnter";
      this.btnEnter.Size = new System.Drawing.Size(207, 64);
      this.btnEnter.TabIndex = 0;
      this.btnEnter.Text = "Enter";
      this.btnEnter.UseVisualStyleBackColor = true;
      this.btnEnter.Click += new System.EventHandler(this.btnEnter_Click);
      // 
      // label1
      // 
      this.label1.AutoSize = true;
      this.label1.Location = new System.Drawing.Point(2, 13);
      this.label1.Name = "label1";
      this.label1.Size = new System.Drawing.Size(124, 13);
      this.label1.TabIndex = 1;
      this.label1.Text = "Ccылка на обьявление";
      // 
      // LinkAdtextBox
      // 
      this.LinkAdtextBox.Location = new System.Drawing.Point(132, 10);
      this.LinkAdtextBox.Name = "LinkAdtextBox";
      this.LinkAdtextBox.Size = new System.Drawing.Size(800, 20);
      this.LinkAdtextBox.TabIndex = 2;
      // 
      // label2
      // 
      this.label2.AutoSize = true;
      this.label2.Location = new System.Drawing.Point(2, 36);
      this.label2.Name = "label2";
      this.label2.Size = new System.Drawing.Size(119, 13);
      this.label2.TabIndex = 3;
      this.label2.Text = "Путь к изображениям";
      // 
      // pathToImgtextBox
      // 
      this.pathToImgtextBox.Location = new System.Drawing.Point(132, 36);
      this.pathToImgtextBox.Name = "pathToImgtextBox";
      this.pathToImgtextBox.Size = new System.Drawing.Size(269, 20);
      this.pathToImgtextBox.TabIndex = 4;
      // 
      // label3
      // 
      this.label3.AutoSize = true;
      this.label3.Location = new System.Drawing.Point(6, 16);
      this.label3.Name = "label3";
      this.label3.Size = new System.Drawing.Size(113, 13);
      this.label3.TabIndex = 5;
      this.label3.Text = "Путь к прокси файлу";
      // 
      // pathToProxytextBox
      // 
      this.pathToProxytextBox.Location = new System.Drawing.Point(127, 13);
      this.pathToProxytextBox.Name = "pathToProxytextBox";
      this.pathToProxytextBox.Size = new System.Drawing.Size(269, 20);
      this.pathToProxytextBox.TabIndex = 6;
      // 
      // groupBox1
      // 
      this.groupBox1.Controls.Add(this.textBoxSleep);
      this.groupBox1.Controls.Add(this.label7);
      this.groupBox1.Controls.Add(this.PasswordtextBox);
      this.groupBox1.Controls.Add(this.userNametextBox);
      this.groupBox1.Controls.Add(this.label5);
      this.groupBox1.Controls.Add(this.label4);
      this.groupBox1.Controls.Add(this.label3);
      this.groupBox1.Controls.Add(this.pathToProxytextBox);
      this.groupBox1.Location = new System.Drawing.Point(5, 109);
      this.groupBox1.Name = "groupBox1";
      this.groupBox1.Size = new System.Drawing.Size(437, 128);
      this.groupBox1.TabIndex = 7;
      this.groupBox1.TabStop = false;
      this.groupBox1.Text = "Прокси";
      this.groupBox1.Enter += new System.EventHandler(this.groupBox1_Enter);
      // 
      // textBoxSleep
      // 
      this.textBoxSleep.Location = new System.Drawing.Point(127, 102);
      this.textBoxSleep.Name = "textBoxSleep";
      this.textBoxSleep.Size = new System.Drawing.Size(100, 20);
      this.textBoxSleep.TabIndex = 12;
      this.textBoxSleep.Text = "200";
      this.textBoxSleep.TextChanged += new System.EventHandler(this.textBoxSleep_TextChanged);
      // 
      // label7
      // 
      this.label7.AutoSize = true;
      this.label7.Location = new System.Drawing.Point(56, 99);
      this.label7.Name = "label7";
      this.label7.Size = new System.Drawing.Size(55, 13);
      this.label7.TabIndex = 11;
      this.label7.Text = "sleep(sec)";
      // 
      // PasswordtextBox
      // 
      this.PasswordtextBox.Location = new System.Drawing.Point(127, 71);
      this.PasswordtextBox.Name = "PasswordtextBox";
      this.PasswordtextBox.Size = new System.Drawing.Size(100, 20);
      this.PasswordtextBox.TabIndex = 10;
      // 
      // userNametextBox
      // 
      this.userNametextBox.Location = new System.Drawing.Point(127, 44);
      this.userNametextBox.Name = "userNametextBox";
      this.userNametextBox.Size = new System.Drawing.Size(100, 20);
      this.userNametextBox.TabIndex = 9;
      // 
      // label5
      // 
      this.label5.AutoSize = true;
      this.label5.Location = new System.Drawing.Point(56, 74);
      this.label5.Name = "label5";
      this.label5.Size = new System.Drawing.Size(53, 13);
      this.label5.TabIndex = 8;
      this.label5.Text = "Password";
      // 
      // label4
      // 
      this.label4.AutoSize = true;
      this.label4.Location = new System.Drawing.Point(56, 44);
      this.label4.Name = "label4";
      this.label4.Size = new System.Drawing.Size(60, 13);
      this.label4.TabIndex = 7;
      this.label4.Text = "User Name";
      // 
      // label6
      // 
      this.label6.AutoSize = true;
      this.label6.Location = new System.Drawing.Point(252, 208);
      this.label6.Name = "label6";
      this.label6.Size = new System.Drawing.Size(0, 13);
      this.label6.TabIndex = 8;
      // 
      // listBox1
      // 
      this.lbCategories.FormattingEnabled = true;
      this.lbCategories.Location = new System.Drawing.Point(432, 36);
      this.lbCategories.Name = "listBox1";
      this.lbCategories.Size = new System.Drawing.Size(500, 212);
      this.lbCategories.TabIndex = 11;
      this.lbCategories.SelectedIndexChanged += new System.EventHandler(this.listBox1_SelectedIndexChanged);
      // 
      // btnParsingAvito
      // 
      this.btnParsingAvito.Location = new System.Drawing.Point(277, 305);
      this.btnParsingAvito.Name = "btnParsingAvito";
      this.btnParsingAvito.Size = new System.Drawing.Size(184, 23);
      this.btnParsingAvito.TabIndex = 12;
      this.btnParsingAvito.Text = "Start Parsing Avito";
      this.btnParsingAvito.UseVisualStyleBackColor = true;
      this.btnParsingAvito.Click += new System.EventHandler(this.btnParsingAvito_Click);
      // 
      // label9
      // 
      this.label9.AutoSize = true;
      this.label9.Location = new System.Drawing.Point(11, 263);
      this.label9.Name = "label9";
      this.label9.Size = new System.Drawing.Size(92, 13);
      this.label9.TabIndex = 14;
      this.label9.Text = "Count Parsed ad: ";
      // 
      // label10
      // 
      this.label10.AutoSize = true;
      this.label10.Location = new System.Drawing.Point(11, 286);
      this.label10.Name = "label10";
      this.label10.Size = new System.Drawing.Size(97, 13);
      this.label10.TabIndex = 15;
      this.label10.Text = "Count Inserted ad: ";
      // 
      // labelParsed
      // 
      this.labelParsed.AutoSize = true;
      this.labelParsed.Location = new System.Drawing.Point(132, 263);
      this.labelParsed.Name = "labelParsed";
      this.labelParsed.Size = new System.Drawing.Size(16, 13);
      this.labelParsed.TabIndex = 16;
      this.labelParsed.Text = "...";
      // 
      // labelInserted
      // 
      this.labelInserted.AutoSize = true;
      this.labelInserted.Location = new System.Drawing.Point(132, 285);
      this.labelInserted.Name = "labelInserted";
      this.labelInserted.Size = new System.Drawing.Size(16, 13);
      this.labelInserted.TabIndex = 17;
      this.labelInserted.Text = "...";
      // 
      // buttonParsingEbay
      // 
      this.buttonParsingEbay.Location = new System.Drawing.Point(277, 341);
      this.buttonParsingEbay.Name = "buttonParsingEbay";
      this.buttonParsingEbay.Size = new System.Drawing.Size(184, 23);
      this.buttonParsingEbay.TabIndex = 18;
      this.buttonParsingEbay.Text = "Start Parsing  Ebay";
      this.buttonParsingEbay.UseVisualStyleBackColor = true;
      this.buttonParsingEbay.Click += new System.EventHandler(this.buttonParsingEbay_Click);
      // 
      // buttonParsingAvitoEbay
      // 
      this.buttonParsingAvitoEbay.Location = new System.Drawing.Point(277, 376);
      this.buttonParsingAvitoEbay.Name = "buttonParsingAvitoEbay";
      this.buttonParsingAvitoEbay.Size = new System.Drawing.Size(184, 23);
      this.buttonParsingAvitoEbay.TabIndex = 19;
      this.buttonParsingAvitoEbay.Text = "Start Parsing Avito+Ebay";
      this.buttonParsingAvitoEbay.UseVisualStyleBackColor = true;
      this.buttonParsingAvitoEbay.Click += new System.EventHandler(this.buttonParsingAvitoEbay_Click);
      // 
      // frmMain
      // 
      this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
      this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
      this.ClientSize = new System.Drawing.Size(944, 411);
      this.Controls.Add(this.buttonParsingAvitoEbay);
      this.Controls.Add(this.buttonParsingEbay);
      this.Controls.Add(this.labelInserted);
      this.Controls.Add(this.labelParsed);
      this.Controls.Add(this.label10);
      this.Controls.Add(this.label9);
      this.Controls.Add(this.btnParsingAvito);
      this.Controls.Add(this.lbCategories);
      this.Controls.Add(this.label6);
      this.Controls.Add(this.groupBox1);
      this.Controls.Add(this.pathToImgtextBox);
      this.Controls.Add(this.label2);
      this.Controls.Add(this.LinkAdtextBox);
      this.Controls.Add(this.label1);
      this.Controls.Add(this.btnEnter);
      this.Name = "frmMain";
      this.Text = "Parser";
      this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.Closing);
      this.Load += new System.EventHandler(this.frmMain_Load);
      this.groupBox1.ResumeLayout(false);
      this.groupBox1.PerformLayout();
      this.ResumeLayout(false);
      this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button btnEnter;
        private System.Windows.Forms.Label label1;
        public System.Windows.Forms.TextBox LinkAdtextBox;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.TextBox pathToImgtextBox;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.TextBox pathToProxytextBox;
        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.TextBox PasswordtextBox;
        private System.Windows.Forms.TextBox userNametextBox;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.ListBox lbCategories;
        private System.Windows.Forms.Button btnParsingAvito;
        private System.Windows.Forms.TextBox textBoxSleep;
        private System.Windows.Forms.Label label7;
        private System.Windows.Forms.Label label9;
        private System.Windows.Forms.Label label10;
        private System.Windows.Forms.Label labelParsed;
        private System.Windows.Forms.Label labelInserted;
        private System.Windows.Forms.Button buttonParsingEbay;
        private System.Windows.Forms.Button buttonParsingAvitoEbay;
    }
}

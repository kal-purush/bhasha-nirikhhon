namespace WindowsFormsApplication1
{
    partial class Main_Form
    {
        /// <summary>
        /// 必需的设计器变量。
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// 清理所有正在使用的资源。
        /// </summary>
        /// <param name="disposing">如果应释放托管资源，为 true；否则为 false。</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows 窗体设计器生成的代码

        /// <summary>
        /// 设计器支持所需的方法 - 不要
        /// 使用代码编辑器修改此方法的内容。
        /// </summary>
        private void InitializeComponent()
        {
            this.splitContainer1 = new System.Windows.Forms.SplitContainer();
            this.button_addExaminer = new System.Windows.Forms.Button();
            this.textBox_caseLocation = new System.Windows.Forms.TextBox();
            this.label8 = new System.Windows.Forms.Label();
            this.textBox_examinerName = new System.Windows.Forms.TextBox();
            this.label7 = new System.Windows.Forms.Label();
            this.textBox_Commander = new System.Windows.Forms.TextBox();
            this.label6 = new System.Windows.Forms.Label();
            this.textBox_endTime = new System.Windows.Forms.TextBox();
            this.textBox2 = new System.Windows.Forms.TextBox();
            this.label5 = new System.Windows.Forms.Label();
            this.textBox_startTime = new System.Windows.Forms.TextBox();
            this.label4 = new System.Windows.Forms.Label();
            this.comboBox_caseReason = new System.Windows.Forms.ComboBox();
            this.label3 = new System.Windows.Forms.Label();
            this.textBox_CaseNumber = new System.Windows.Forms.TextBox();
            this.label2 = new System.Windows.Forms.Label();
            this.ComboBox_peopleKind = new System.Windows.Forms.ComboBox();
            this.人员类型 = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.menuStrip1 = new System.Windows.Forms.MenuStrip();
            this.toolStripMenuItem1 = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripMenuItem2 = new System.Windows.Forms.ToolStripMenuItem();
            this.button_submit = new System.Windows.Forms.Button();
            this.splitContainer1.Panel1.SuspendLayout();
            this.splitContainer1.SuspendLayout();
            this.menuStrip1.SuspendLayout();
            this.SuspendLayout();
            // 
            // splitContainer1
            // 
            this.splitContainer1.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom)
                        | System.Windows.Forms.AnchorStyles.Left)
                        | System.Windows.Forms.AnchorStyles.Right)));
            this.splitContainer1.Location = new System.Drawing.Point(12, 28);
            this.splitContainer1.Name = "splitContainer1";
            // 
            // splitContainer1.Panel1
            // 
            this.splitContainer1.Panel1.Controls.Add(this.button_submit);
            this.splitContainer1.Panel1.Controls.Add(this.button_addExaminer);
            this.splitContainer1.Panel1.Controls.Add(this.textBox_caseLocation);
            this.splitContainer1.Panel1.Controls.Add(this.label8);
            this.splitContainer1.Panel1.Controls.Add(this.textBox_examinerName);
            this.splitContainer1.Panel1.Controls.Add(this.label7);
            this.splitContainer1.Panel1.Controls.Add(this.textBox_Commander);
            this.splitContainer1.Panel1.Controls.Add(this.label6);
            this.splitContainer1.Panel1.Controls.Add(this.textBox_endTime);
            this.splitContainer1.Panel1.Controls.Add(this.textBox2);
            this.splitContainer1.Panel1.Controls.Add(this.label5);
            this.splitContainer1.Panel1.Controls.Add(this.textBox_startTime);
            this.splitContainer1.Panel1.Controls.Add(this.label4);
            this.splitContainer1.Panel1.Controls.Add(this.comboBox_caseReason);
            this.splitContainer1.Panel1.Controls.Add(this.label3);
            this.splitContainer1.Panel1.Controls.Add(this.textBox_CaseNumber);
            this.splitContainer1.Panel1.Controls.Add(this.label2);
            this.splitContainer1.Panel1.Controls.Add(this.ComboBox_peopleKind);
            this.splitContainer1.Panel1.Controls.Add(this.人员类型);
            this.splitContainer1.Panel1.Controls.Add(this.label1);
            this.splitContainer1.Panel1.Paint += new System.Windows.Forms.PaintEventHandler(this.splitContainer1_Panel1_Paint);
            this.splitContainer1.Size = new System.Drawing.Size(742, 409);
            this.splitContainer1.SplitterDistance = 514;
            this.splitContainer1.TabIndex = 2;
            // 
            // button_addExaminer
            // 
            this.button_addExaminer.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Zoom;
            this.button_addExaminer.Location = new System.Drawing.Point(190, 279);
            this.button_addExaminer.Name = "button_addExaminer";
            this.button_addExaminer.Size = new System.Drawing.Size(75, 23);
            this.button_addExaminer.TabIndex = 19;
            this.button_addExaminer.Text = " ";
            this.button_addExaminer.UseVisualStyleBackColor = true;
            this.button_addExaminer.Click += new System.EventHandler(this.button_addExaminer_Click);
            // 
            // textBox_caseLocation
            // 
            this.textBox_caseLocation.Location = new System.Drawing.Point(84, 324);
            this.textBox_caseLocation.Name = "textBox_caseLocation";
            this.textBox_caseLocation.Size = new System.Drawing.Size(100, 21);
            this.textBox_caseLocation.TabIndex = 18;
            // 
            // label8
            // 
            this.label8.AutoSize = true;
            this.label8.Location = new System.Drawing.Point(13, 327);
            this.label8.Name = "label8";
            this.label8.Size = new System.Drawing.Size(65, 12);
            this.label8.TabIndex = 17;
            this.label8.Text = "勘验地点：";
            // 
            // textBox_examinerName
            // 
            this.textBox_examinerName.Location = new System.Drawing.Point(84, 279);
            this.textBox_examinerName.Name = "textBox_examinerName";
            this.textBox_examinerName.Size = new System.Drawing.Size(100, 21);
            this.textBox_examinerName.TabIndex = 16;
            // 
            // label7
            // 
            this.label7.AutoSize = true;
            this.label7.Location = new System.Drawing.Point(13, 282);
            this.label7.Name = "label7";
            this.label7.Size = new System.Drawing.Size(65, 12);
            this.label7.TabIndex = 15;
            this.label7.Text = "勘验人员：";
            // 
            // textBox_Commander
            // 
            this.textBox_Commander.Location = new System.Drawing.Point(84, 232);
            this.textBox_Commander.Name = "textBox_Commander";
            this.textBox_Commander.Size = new System.Drawing.Size(100, 21);
            this.textBox_Commander.TabIndex = 14;
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Location = new System.Drawing.Point(13, 235);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(77, 12);
            this.label6.TabIndex = 13;
            this.label6.Text = "勘验指挥官：";
            // 
            // textBox_endTime
            // 
            this.textBox_endTime.Location = new System.Drawing.Point(72, 181);
            this.textBox_endTime.Name = "textBox_endTime";
            this.textBox_endTime.Size = new System.Drawing.Size(100, 21);
            this.textBox_endTime.TabIndex = 12;
            this.textBox_endTime.TextChanged += new System.EventHandler(this.textBox_endTime_TextChanged);
            // 
            // textBox2
            // 
            this.textBox2.Location = new System.Drawing.Point(252, 37);
            this.textBox2.Name = "textBox2";
            this.textBox2.Size = new System.Drawing.Size(100, 21);
            this.textBox2.TabIndex = 11;
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Location = new System.Drawing.Point(12, 187);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(65, 12);
            this.label5.TabIndex = 10;
            this.label5.Text = "结束时间：";
            // 
            // textBox_startTime
            // 
            this.textBox_startTime.Location = new System.Drawing.Point(72, 135);
            this.textBox_startTime.Name = "textBox_startTime";
            this.textBox_startTime.Size = new System.Drawing.Size(100, 21);
            this.textBox_startTime.TabIndex = 9;
            this.textBox_startTime.TextChanged += new System.EventHandler(this.textBox_startTime_TextChanged);
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(13, 141);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(65, 12);
            this.label4.TabIndex = 8;
            this.label4.Text = "开始时间：";
            // 
            // comboBox_caseReason
            // 
            this.comboBox_caseReason.FormattingEnabled = true;
            this.comboBox_caseReason.Location = new System.Drawing.Point(72, 88);
            this.comboBox_caseReason.Name = "comboBox_caseReason";
            this.comboBox_caseReason.Size = new System.Drawing.Size(80, 20);
            this.comboBox_caseReason.TabIndex = 7;
            this.comboBox_caseReason.SelectedIndexChanged += new System.EventHandler(this.comboBox_caseReason_SelectedIndexChanged);
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(13, 91);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(53, 12);
            this.label3.TabIndex = 6;
            this.label3.Text = "到案类型";
            // 
            // textBox_CaseNumber
            // 
            this.textBox_CaseNumber.Location = new System.Drawing.Point(264, 88);
            this.textBox_CaseNumber.Name = "textBox_CaseNumber";
            this.textBox_CaseNumber.Size = new System.Drawing.Size(100, 21);
            this.textBox_CaseNumber.TabIndex = 5;
            this.textBox_CaseNumber.TextChanged += new System.EventHandler(this.textBox_CaseNumber_TextChanged);
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(167, 91);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(101, 12);
            this.label2.TabIndex = 4;
            this.label2.Text = "案件编号或名称：";
            // 
            // ComboBox_peopleKind
            // 
            this.ComboBox_peopleKind.FormattingEnabled = true;
            this.ComboBox_peopleKind.Location = new System.Drawing.Point(72, 37);
            this.ComboBox_peopleKind.Name = "ComboBox_peopleKind";
            this.ComboBox_peopleKind.Size = new System.Drawing.Size(80, 20);
            this.ComboBox_peopleKind.TabIndex = 3;
            this.ComboBox_peopleKind.SelectedIndexChanged += new System.EventHandler(this.ComboBox_peopleKind_SelectedIndexChanged);
            // 
            // 人员类型
            // 
            this.人员类型.AutoSize = true;
            this.人员类型.Location = new System.Drawing.Point(167, 40);
            this.人员类型.Name = "人员类型";
            this.人员类型.Size = new System.Drawing.Size(89, 12);
            this.人员类型.TabIndex = 1;
            this.人员类型.Text = "被调查人姓名：";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(13, 40);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(53, 12);
            this.label1.TabIndex = 0;
            this.label1.Text = "人员类型";
            this.label1.Click += new System.EventHandler(this.label1_Click_1);
            // 
            // menuStrip1
            // 
            this.menuStrip1.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.toolStripMenuItem1,
            this.toolStripMenuItem2});
            this.menuStrip1.Location = new System.Drawing.Point(0, 0);
            this.menuStrip1.Name = "menuStrip1";
            this.menuStrip1.Size = new System.Drawing.Size(766, 25);
            this.menuStrip1.TabIndex = 0;
            this.menuStrip1.Text = "menuStrip1";
            this.menuStrip1.ItemClicked += new System.Windows.Forms.ToolStripItemClickedEventHandler(this.menuStrip1_ItemClicked);
            // 
            // toolStripMenuItem1
            // 
            this.toolStripMenuItem1.Name = "toolStripMenuItem1";
            this.toolStripMenuItem1.Size = new System.Drawing.Size(27, 21);
            this.toolStripMenuItem1.Text = "1";
            // 
            // toolStripMenuItem2
            // 
            this.toolStripMenuItem2.Name = "toolStripMenuItem2";
            this.toolStripMenuItem2.Size = new System.Drawing.Size(27, 21);
            this.toolStripMenuItem2.Text = "2";
            // 
            // button_submit
            // 
            this.button_submit.Location = new System.Drawing.Point(181, 368);
            this.button_submit.Name = "button_submit";
            this.button_submit.Size = new System.Drawing.Size(75, 23);
            this.button_submit.TabIndex = 20;
            this.button_submit.Text = "提交";
            this.button_submit.UseVisualStyleBackColor = true;
            this.button_submit.Click += new System.EventHandler(this.button_submit_Click);
            // 
            // Main_Form
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 12F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(766, 449);
            this.Controls.Add(this.splitContainer1);
            this.Controls.Add(this.menuStrip1);
            this.MainMenuStrip = this.menuStrip1;
            this.Name = "Main_Form";
            this.Text = "Form1";
            this.Load += new System.EventHandler(this.Main_Form_Load);
            this.splitContainer1.Panel1.ResumeLayout(false);
            this.splitContainer1.Panel1.PerformLayout();
            this.splitContainer1.ResumeLayout(false);
            this.menuStrip1.ResumeLayout(false);
            this.menuStrip1.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.SplitContainer splitContainer1;
        private System.Windows.Forms.MenuStrip menuStrip1;
        private System.Windows.Forms.ToolStripMenuItem toolStripMenuItem1;
        private System.Windows.Forms.ToolStripMenuItem toolStripMenuItem2;
        private System.Windows.Forms.ComboBox ComboBox_peopleKind;
        private System.Windows.Forms.Label 人员类型;
        private System.Windows.Forms.TextBox textBox_CaseNumber;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.ComboBox comboBox_caseReason;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.TextBox textBox_endTime;
        private System.Windows.Forms.TextBox textBox2;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.TextBox textBox_startTime;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.TextBox textBox_examinerName;
        private System.Windows.Forms.Label label7;
        private System.Windows.Forms.TextBox textBox_Commander;
        private System.Windows.Forms.TextBox textBox_caseLocation;
        private System.Windows.Forms.Label label8;
        private System.Windows.Forms.Button button_addExaminer;
        private System.Windows.Forms.Button button_submit;

    }
}

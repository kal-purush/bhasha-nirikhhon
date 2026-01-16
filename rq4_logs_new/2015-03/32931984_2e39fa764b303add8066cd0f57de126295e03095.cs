namespace ApplicationLogAnalyzer
{
    partial class AnalyzerForm
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
            this.openLogFileDialog = new System.Windows.Forms.OpenFileDialog();
            this.btnOpenLog = new System.Windows.Forms.Button();
            this.dataGridView1 = new System.Windows.Forms.DataGridView();
            this.Level = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.Message = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.DateTime = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.dpStart = new System.Windows.Forms.DateTimePicker();
            this.dpEnd = new System.Windows.Forms.DateTimePicker();
            this.label1 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.gbSeverity = new System.Windows.Forms.GroupBox();
            this.rbError = new System.Windows.Forms.RadioButton();
            this.rbWarning = new System.Windows.Forms.RadioButton();
            this.rbSuccess = new System.Windows.Forms.RadioButton();
            this.rbInfo = new System.Windows.Forms.RadioButton();
            this.rbDebug = new System.Windows.Forms.RadioButton();
            this.rbAll = new System.Windows.Forms.RadioButton();
            ((System.ComponentModel.ISupportInitialize)(this.dataGridView1)).BeginInit();
            this.gbSeverity.SuspendLayout();
            this.SuspendLayout();
            // 
            // openLogFileDialog
            // 
            this.openLogFileDialog.FileName = "openFileDialog1";
            this.openLogFileDialog.InitialDirectory = "%USERPROFILE%";
            this.openLogFileDialog.FileOk += new System.ComponentModel.CancelEventHandler(this.openLogFileDialog_FileOk_1);
            // 
            // btnOpenLog
            // 
            this.btnOpenLog.Location = new System.Drawing.Point(12, 10);
            this.btnOpenLog.Name = "btnOpenLog";
            this.btnOpenLog.Size = new System.Drawing.Size(200, 23);
            this.btnOpenLog.TabIndex = 0;
            this.btnOpenLog.Text = "Open Log File";
            this.btnOpenLog.UseVisualStyleBackColor = true;
            this.btnOpenLog.Click += new System.EventHandler(this.button1_Click);
            // 
            // dataGridView1
            // 
            this.dataGridView1.AllowUserToAddRows = false;
            this.dataGridView1.AllowUserToDeleteRows = false;
            this.dataGridView1.AllowUserToOrderColumns = true;
            this.dataGridView1.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dataGridView1.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.Level,
            this.Message,
            this.DateTime});
            this.dataGridView1.ImeMode = System.Windows.Forms.ImeMode.NoControl;
            this.dataGridView1.Location = new System.Drawing.Point(218, 1);
            this.dataGridView1.Name = "dataGridView1";
            this.dataGridView1.ReadOnly = true;
            this.dataGridView1.RowHeadersWidthSizeMode = System.Windows.Forms.DataGridViewRowHeadersWidthSizeMode.AutoSizeToAllHeaders;
            this.dataGridView1.ShowCellToolTips = false;
            this.dataGridView1.ShowEditingIcon = false;
            this.dataGridView1.ShowRowErrors = false;
            this.dataGridView1.Size = new System.Drawing.Size(600, 298);
            this.dataGridView1.TabIndex = 2;
            this.dataGridView1.CellContentClick += new System.Windows.Forms.DataGridViewCellEventHandler(this.dataGridView1_CellContentClick);
            // 
            // Level
            // 
            this.Level.HeaderText = "Level";
            this.Level.Name = "Level";
            this.Level.ReadOnly = true;
            // 
            // Message
            // 
            this.Message.HeaderText = "Message";
            this.Message.Name = "Message";
            this.Message.ReadOnly = true;
            // 
            // DateTime
            // 
            this.DateTime.HeaderText = "Date";
            this.DateTime.Name = "DateTime";
            this.DateTime.ReadOnly = true;
            // 
            // dpStart
            // 
            this.dpStart.Location = new System.Drawing.Point(12, 63);
            this.dpStart.Name = "dpStart";
            this.dpStart.Size = new System.Drawing.Size(200, 20);
            this.dpStart.TabIndex = 5;
            this.dpStart.Value = new System.DateTime(1970, 1, 1, 14, 24, 0, 0);
            this.dpStart.ValueChanged += new System.EventHandler(this.ConstraintControlChanged);
            // 
            // dpEnd
            // 
            this.dpEnd.Location = new System.Drawing.Point(12, 102);
            this.dpEnd.Name = "dpEnd";
            this.dpEnd.Size = new System.Drawing.Size(200, 20);
            this.dpEnd.TabIndex = 6;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(13, 44);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(55, 13);
            this.label1.TabIndex = 8;
            this.label1.Text = "Start Date";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(12, 86);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(52, 13);
            this.label2.TabIndex = 9;
            this.label2.Text = "End Date";
            // 
            // gbSeverity
            // 
            this.gbSeverity.Controls.Add(this.rbError);
            this.gbSeverity.Controls.Add(this.rbWarning);
            this.gbSeverity.Controls.Add(this.rbSuccess);
            this.gbSeverity.Controls.Add(this.rbInfo);
            this.gbSeverity.Controls.Add(this.rbDebug);
            this.gbSeverity.Controls.Add(this.rbAll);
            this.gbSeverity.Location = new System.Drawing.Point(12, 128);
            this.gbSeverity.Name = "gbSeverity";
            this.gbSeverity.Size = new System.Drawing.Size(200, 171);
            this.gbSeverity.TabIndex = 11;
            this.gbSeverity.TabStop = false;
            this.gbSeverity.Text = "Message Severity";
            // 
            // rbError
            // 
            this.rbError.AutoSize = true;
            this.rbError.Location = new System.Drawing.Point(7, 140);
            this.rbError.Name = "rbError";
            this.rbError.Size = new System.Drawing.Size(47, 17);
            this.rbError.TabIndex = 5;
            this.rbError.Text = "Error";
            this.rbError.UseVisualStyleBackColor = true;
            this.rbError.CheckedChanged += new System.EventHandler(this.ConstraintControlChanged);
            // 
            // rbWarning
            // 
            this.rbWarning.AutoSize = true;
            this.rbWarning.Location = new System.Drawing.Point(7, 116);
            this.rbWarning.Name = "rbWarning";
            this.rbWarning.Size = new System.Drawing.Size(65, 17);
            this.rbWarning.TabIndex = 4;
            this.rbWarning.Text = "Warning";
            this.rbWarning.UseVisualStyleBackColor = true;
            this.rbWarning.CheckedChanged += new System.EventHandler(this.ConstraintControlChanged);
            // 
            // rbSuccess
            // 
            this.rbSuccess.AutoSize = true;
            this.rbSuccess.Location = new System.Drawing.Point(7, 92);
            this.rbSuccess.Name = "rbSuccess";
            this.rbSuccess.Size = new System.Drawing.Size(66, 17);
            this.rbSuccess.TabIndex = 3;
            this.rbSuccess.Text = "Success";
            this.rbSuccess.UseVisualStyleBackColor = true;
            this.rbSuccess.CheckedChanged += new System.EventHandler(this.ConstraintControlChanged);
            // 
            // rbInfo
            // 
            this.rbInfo.AutoSize = true;
            this.rbInfo.Location = new System.Drawing.Point(7, 67);
            this.rbInfo.Name = "rbInfo";
            this.rbInfo.Size = new System.Drawing.Size(43, 17);
            this.rbInfo.TabIndex = 2;
            this.rbInfo.Text = "Info";
            this.rbInfo.UseVisualStyleBackColor = true;
            this.rbInfo.CheckedChanged += new System.EventHandler(this.ConstraintControlChanged);
            // 
            // rbDebug
            // 
            this.rbDebug.AutoSize = true;
            this.rbDebug.Location = new System.Drawing.Point(7, 44);
            this.rbDebug.Name = "rbDebug";
            this.rbDebug.Size = new System.Drawing.Size(57, 17);
            this.rbDebug.TabIndex = 1;
            this.rbDebug.Text = "Debug";
            this.rbDebug.UseVisualStyleBackColor = true;
            this.rbDebug.CheckedChanged += new System.EventHandler(this.ConstraintControlChanged);
            // 
            // rbAll
            // 
            this.rbAll.AutoSize = true;
            this.rbAll.Checked = true;
            this.rbAll.Location = new System.Drawing.Point(7, 20);
            this.rbAll.Name = "rbAll";
            this.rbAll.Size = new System.Drawing.Size(87, 17);
            this.rbAll.TabIndex = 0;
            this.rbAll.TabStop = true;
            this.rbAll.Text = "All Messages";
            this.rbAll.UseVisualStyleBackColor = true;
            this.rbAll.CheckedChanged += new System.EventHandler(this.ConstraintControlChanged);
            // 
            // AnalyzerForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(817, 304);
            this.Controls.Add(this.gbSeverity);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.dpEnd);
            this.Controls.Add(this.dpStart);
            this.Controls.Add(this.dataGridView1);
            this.Controls.Add(this.btnOpenLog);
            this.Name = "AnalyzerForm";
            this.Text = "Application Log Analyzer";
            this.Load += new System.EventHandler(this.Form1_Load);
            ((System.ComponentModel.ISupportInitialize)(this.dataGridView1)).EndInit();
            this.gbSeverity.ResumeLayout(false);
            this.gbSeverity.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.OpenFileDialog openLogFileDialog;
        private System.Windows.Forms.Button btnOpenLog;
        private System.Windows.Forms.DataGridView dataGridView1;
        private System.Windows.Forms.DataGridViewTextBoxColumn Level;
        private System.Windows.Forms.DataGridViewTextBoxColumn Message;
        private System.Windows.Forms.DataGridViewTextBoxColumn DateTime;
        private System.Windows.Forms.DateTimePicker dpStart;
        private System.Windows.Forms.DateTimePicker dpEnd;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.GroupBox gbSeverity;
        private System.Windows.Forms.RadioButton rbError;
        private System.Windows.Forms.RadioButton rbWarning;
        private System.Windows.Forms.RadioButton rbSuccess;
        private System.Windows.Forms.RadioButton rbInfo;
        private System.Windows.Forms.RadioButton rbDebug;
        private System.Windows.Forms.RadioButton rbAll;
    }
}

namespace Scanner
{
    partial class Form1
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
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle1 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle2 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle3 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle4 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle5 = new System.Windows.Forms.DataGridViewCellStyle();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle6 = new System.Windows.Forms.DataGridViewCellStyle();
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(Form1));
            this.CodeBox = new System.Windows.Forms.TextBox();
            this.TokensTable = new System.Windows.Forms.DataGridView();
            this.LexemesColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.TokensColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.ScanButton = new System.Windows.Forms.Button();
            this.label1 = new System.Windows.Forms.Label();
            this.UploadCodeButton = new System.Windows.Forms.Button();
            this.ClearButton = new System.Windows.Forms.Button();
            this.ErrorsTable = new System.Windows.Forms.DataGridView();
            this.ErrorsColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.ErrorTypeColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.label3 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            ((System.ComponentModel.ISupportInitialize)(this.TokensTable)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.ErrorsTable)).BeginInit();
            this.SuspendLayout();
            // 
            // CodeBox
            // 
            this.CodeBox.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.CodeBox.Font = new System.Drawing.Font("Consolas", 14F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Pixel, ((byte)(0)));
            this.CodeBox.Location = new System.Drawing.Point(50, 109);
            this.CodeBox.Multiline = true;
            this.CodeBox.Name = "CodeBox";
            this.CodeBox.Size = new System.Drawing.Size(380, 366);
            this.CodeBox.TabIndex = 0;
            // 
            // TokensTable
            // 
            this.TokensTable.AllowUserToAddRows = false;
            this.TokensTable.AllowUserToDeleteRows = false;
            this.TokensTable.AllowUserToOrderColumns = true;
            this.TokensTable.AllowUserToResizeColumns = false;
            this.TokensTable.AllowUserToResizeRows = false;
            this.TokensTable.AutoSizeColumnsMode = System.Windows.Forms.DataGridViewAutoSizeColumnsMode.AllCells;
            this.TokensTable.AutoSizeRowsMode = System.Windows.Forms.DataGridViewAutoSizeRowsMode.AllCells;
            this.TokensTable.BackgroundColor = System.Drawing.Color.Lavender;
            this.TokensTable.ColumnHeadersBorderStyle = System.Windows.Forms.DataGridViewHeaderBorderStyle.Single;
            dataGridViewCellStyle1.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleCenter;
            dataGridViewCellStyle1.BackColor = System.Drawing.Color.SteelBlue;
            dataGridViewCellStyle1.Font = new System.Drawing.Font("Microsoft Sans Serif", 18F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            dataGridViewCellStyle1.ForeColor = System.Drawing.SystemColors.Info;
            dataGridViewCellStyle1.SelectionBackColor = System.Drawing.SystemColors.Highlight;
            dataGridViewCellStyle1.SelectionForeColor = System.Drawing.SystemColors.HighlightText;
            dataGridViewCellStyle1.WrapMode = System.Windows.Forms.DataGridViewTriState.True;
            this.TokensTable.ColumnHeadersDefaultCellStyle = dataGridViewCellStyle1;
            this.TokensTable.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.TokensTable.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.LexemesColumn,
            this.TokensColumn});
            this.TokensTable.EnableHeadersVisualStyles = false;
            this.TokensTable.GridColor = System.Drawing.Color.Black;
            this.TokensTable.Location = new System.Drawing.Point(480, 20);
            this.TokensTable.MultiSelect = false;
            this.TokensTable.Name = "TokensTable";
            this.TokensTable.ReadOnly = true;
            this.TokensTable.RowHeadersVisible = false;
            dataGridViewCellStyle2.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleCenter;
            dataGridViewCellStyle2.BackColor = System.Drawing.SystemColors.Window;
            dataGridViewCellStyle2.Font = new System.Drawing.Font("Microsoft Sans Serif", 14F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Pixel, ((byte)(0)));
            dataGridViewCellStyle2.ForeColor = System.Drawing.SystemColors.WindowText;
            dataGridViewCellStyle2.SelectionBackColor = System.Drawing.SystemColors.Highlight;
            dataGridViewCellStyle2.SelectionForeColor = System.Drawing.SystemColors.HighlightText;
            dataGridViewCellStyle2.WrapMode = System.Windows.Forms.DataGridViewTriState.True;
            this.TokensTable.RowsDefaultCellStyle = dataGridViewCellStyle2;
            this.TokensTable.RowTemplate.Resizable = System.Windows.Forms.DataGridViewTriState.False;
            this.TokensTable.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.TokensTable.Size = new System.Drawing.Size(350, 350);
            this.TokensTable.TabIndex = 2;
            this.TokensTable.CellContentClick += new System.Windows.Forms.DataGridViewCellEventHandler(this.TokensTable_CellContentClick);
            // 
            // LexemesColumn
            // 
            this.LexemesColumn.AutoSizeMode = System.Windows.Forms.DataGridViewAutoSizeColumnMode.Fill;
            this.LexemesColumn.FillWeight = 2F;
            this.LexemesColumn.HeaderText = "Lexeme";
            this.LexemesColumn.MinimumWidth = 6;
            this.LexemesColumn.Name = "LexemesColumn";
            this.LexemesColumn.ReadOnly = true;
            // 
            // TokensColumn
            // 
            this.TokensColumn.AutoSizeMode = System.Windows.Forms.DataGridViewAutoSizeColumnMode.Fill;
            this.TokensColumn.FillWeight = 3F;
            this.TokensColumn.HeaderText = "Token";
            this.TokensColumn.MinimumWidth = 6;
            this.TokensColumn.Name = "TokensColumn";
            this.TokensColumn.ReadOnly = true;
            // 
            // ScanButton
            // 
            this.ScanButton.AutoSize = true;
            this.ScanButton.AutoSizeMode = System.Windows.Forms.AutoSizeMode.GrowAndShrink;
            this.ScanButton.Font = new System.Drawing.Font("Microsoft Sans Serif", 14F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Pixel);
            this.ScanButton.Location = new System.Drawing.Point(116, 483);
            this.ScanButton.Name = "ScanButton";
            this.ScanButton.Size = new System.Drawing.Size(87, 27);
            this.ScanButton.TabIndex = 3;
            this.ScanButton.Text = "Scan Code";
            this.ScanButton.Click += new System.EventHandler(this.ScanButton_Click);
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.BackColor = System.Drawing.SystemColors.InactiveCaption;
            this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 18F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Pixel, ((byte)(0)));
            this.label1.Location = new System.Drawing.Point(46, 73);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(239, 22);
            this.label1.TabIndex = 4;
            this.label1.Text = "Option 2: Manually type here";
            this.label1.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.label1.Click += new System.EventHandler(this.label1_Click);
            // 
            // UploadCodeButton
            // 
            this.UploadCodeButton.AutoSize = true;
            this.UploadCodeButton.AutoSizeMode = System.Windows.Forms.AutoSizeMode.GrowAndShrink;
            this.UploadCodeButton.FlatStyle = System.Windows.Forms.FlatStyle.System;
            this.UploadCodeButton.Font = new System.Drawing.Font("Microsoft Sans Serif", 16F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Pixel);
            this.UploadCodeButton.Location = new System.Drawing.Point(135, 21);
            this.UploadCodeButton.Name = "UploadCodeButton";
            this.UploadCodeButton.Size = new System.Drawing.Size(145, 29);
            this.UploadCodeButton.TabIndex = 6;
            this.UploadCodeButton.Text = "Upload Code File";
            this.UploadCodeButton.Click += new System.EventHandler(this.UploadCodeButton_Click);
            // 
            // ClearButton
            // 
            this.ClearButton.AutoSize = true;
            this.ClearButton.AutoSizeMode = System.Windows.Forms.AutoSizeMode.GrowAndShrink;
            this.ClearButton.Font = new System.Drawing.Font("Microsoft Sans Serif", 14F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Pixel);
            this.ClearButton.Location = new System.Drawing.Point(209, 483);
            this.ClearButton.Name = "ClearButton";
            this.ClearButton.Size = new System.Drawing.Size(104, 27);
            this.ClearButton.TabIndex = 7;
            this.ClearButton.Text = "Clear Window";
            this.ClearButton.Click += new System.EventHandler(this.ClearButton_Click);
            // 
            // ErrorsTable
            // 
            this.ErrorsTable.AllowUserToAddRows = false;
            this.ErrorsTable.AllowUserToDeleteRows = false;
            this.ErrorsTable.AllowUserToOrderColumns = true;
            this.ErrorsTable.AllowUserToResizeColumns = false;
            this.ErrorsTable.AllowUserToResizeRows = false;
            this.ErrorsTable.AutoSizeColumnsMode = System.Windows.Forms.DataGridViewAutoSizeColumnsMode.AllCells;
            this.ErrorsTable.AutoSizeRowsMode = System.Windows.Forms.DataGridViewAutoSizeRowsMode.AllCells;
            this.ErrorsTable.BackgroundColor = System.Drawing.Color.Lavender;
            this.ErrorsTable.ColumnHeadersBorderStyle = System.Windows.Forms.DataGridViewHeaderBorderStyle.Single;
            dataGridViewCellStyle3.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleCenter;
            dataGridViewCellStyle3.BackColor = System.Drawing.Color.SteelBlue;
            dataGridViewCellStyle3.Font = new System.Drawing.Font("Microsoft Sans Serif", 18F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Pixel, ((byte)(0)));
            dataGridViewCellStyle3.ForeColor = System.Drawing.SystemColors.Info;
            dataGridViewCellStyle3.SelectionBackColor = System.Drawing.SystemColors.Highlight;
            dataGridViewCellStyle3.SelectionForeColor = System.Drawing.SystemColors.HighlightText;
            dataGridViewCellStyle3.WrapMode = System.Windows.Forms.DataGridViewTriState.True;
            this.ErrorsTable.ColumnHeadersDefaultCellStyle = dataGridViewCellStyle3;
            this.ErrorsTable.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.ErrorsTable.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.ErrorsColumn,
            this.ErrorTypeColumn});
            dataGridViewCellStyle4.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleLeft;
            dataGridViewCellStyle4.BackColor = System.Drawing.SystemColors.Window;
            dataGridViewCellStyle4.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            dataGridViewCellStyle4.ForeColor = System.Drawing.Color.Black;
            dataGridViewCellStyle4.SelectionBackColor = System.Drawing.SystemColors.Highlight;
            dataGridViewCellStyle4.SelectionForeColor = System.Drawing.SystemColors.HighlightText;
            dataGridViewCellStyle4.WrapMode = System.Windows.Forms.DataGridViewTriState.False;
            this.ErrorsTable.DefaultCellStyle = dataGridViewCellStyle4;
            this.ErrorsTable.EnableHeadersVisualStyles = false;
            this.ErrorsTable.GridColor = System.Drawing.Color.Black;
            this.ErrorsTable.Location = new System.Drawing.Point(480, 380);
            this.ErrorsTable.MultiSelect = false;
            this.ErrorsTable.Name = "ErrorsTable";
            this.ErrorsTable.ReadOnly = true;
            dataGridViewCellStyle5.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleLeft;
            dataGridViewCellStyle5.BackColor = System.Drawing.SystemColors.Control;
            dataGridViewCellStyle5.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            dataGridViewCellStyle5.ForeColor = System.Drawing.SystemColors.Window;
            dataGridViewCellStyle5.SelectionBackColor = System.Drawing.SystemColors.Highlight;
            dataGridViewCellStyle5.SelectionForeColor = System.Drawing.SystemColors.HighlightText;
            dataGridViewCellStyle5.WrapMode = System.Windows.Forms.DataGridViewTriState.True;
            this.ErrorsTable.RowHeadersDefaultCellStyle = dataGridViewCellStyle5;
            this.ErrorsTable.RowHeadersVisible = false;
            this.ErrorsTable.RowHeadersWidth = 51;
            dataGridViewCellStyle6.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleCenter;
            dataGridViewCellStyle6.BackColor = System.Drawing.SystemColors.Window;
            dataGridViewCellStyle6.Font = new System.Drawing.Font("Microsoft Sans Serif", 14F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Pixel, ((byte)(0)));
            dataGridViewCellStyle6.ForeColor = System.Drawing.SystemColors.WindowText;
            dataGridViewCellStyle6.SelectionBackColor = System.Drawing.SystemColors.Highlight;
            dataGridViewCellStyle6.SelectionForeColor = System.Drawing.SystemColors.HighlightText;
            dataGridViewCellStyle6.WrapMode = System.Windows.Forms.DataGridViewTriState.True;
            this.ErrorsTable.RowsDefaultCellStyle = dataGridViewCellStyle6;
            this.ErrorsTable.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.ErrorsTable.Size = new System.Drawing.Size(350, 130);
            this.ErrorsTable.TabIndex = 8;
            // 
            // ErrorsColumn
            // 
            this.ErrorsColumn.AutoSizeMode = System.Windows.Forms.DataGridViewAutoSizeColumnMode.Fill;
            this.ErrorsColumn.FillWeight = 2F;
            this.ErrorsColumn.HeaderText = "Error";
            this.ErrorsColumn.MinimumWidth = 6;
            this.ErrorsColumn.Name = "ErrorsColumn";
            this.ErrorsColumn.ReadOnly = true;
            // 
            // ErrorTypeColumn
            // 
            this.ErrorTypeColumn.AutoSizeMode = System.Windows.Forms.DataGridViewAutoSizeColumnMode.Fill;
            this.ErrorTypeColumn.FillWeight = 3F;
            this.ErrorTypeColumn.HeaderText = "Error Type";
            this.ErrorTypeColumn.MinimumWidth = 6;
            this.ErrorTypeColumn.Name = "ErrorTypeColumn";
            this.ErrorTypeColumn.ReadOnly = true;
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.BackColor = System.Drawing.Color.Black;
            this.label3.Font = new System.Drawing.Font("Microsoft Sans Serif", 14F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Pixel, ((byte)(0)));
            this.label3.ForeColor = System.Drawing.Color.DarkGray;
            this.label3.Location = new System.Drawing.Point(335, 530);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(224, 17);
            this.label3.TabIndex = 9;
            this.label3.Text = "Ver 1.3.0504     All rights reserved";
            this.label3.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.BackColor = System.Drawing.SystemColors.InactiveCaption;
            this.label2.Font = new System.Drawing.Font("Microsoft Sans Serif", 18F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Pixel, ((byte)(0)));
            this.label2.Location = new System.Drawing.Point(46, 24);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(83, 22);
            this.label2.TabIndex = 10;
            this.label2.Text = "Option 1:";
            this.label2.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // Form1
            // 
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.None;
            this.AutoSizeMode = System.Windows.Forms.AutoSizeMode.GrowAndShrink;
            this.BackgroundImage = global::Scanner.Properties.Resources.Blue_Code;
            this.ClientSize = new System.Drawing.Size(884, 561);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.ErrorsTable);
            this.Controls.Add(this.ClearButton);
            this.Controls.Add(this.UploadCodeButton);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.ScanButton);
            this.Controls.Add(this.TokensTable);
            this.Controls.Add(this.CodeBox);
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.MaximizeBox = false;
            this.MaximumSize = new System.Drawing.Size(900, 600);
            this.MinimumSize = new System.Drawing.Size(900, 600);
            this.Name = "Form1";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Scanner Program";
            this.Load += new System.EventHandler(this.Form1_Load);
            ((System.ComponentModel.ISupportInitialize)(this.TokensTable)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.ErrorsTable)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox CodeBox;
        private System.Windows.Forms.DataGridView TokensTable;
        private System.Windows.Forms.Button ScanButton;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Button UploadCodeButton;
        private System.Windows.Forms.Button ClearButton;
        private System.Windows.Forms.DataGridView ErrorsTable;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.DataGridViewTextBoxColumn LexemesColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn TokensColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn ErrorsColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn ErrorTypeColumn;
        private System.Windows.Forms.Label label2;
    }
}

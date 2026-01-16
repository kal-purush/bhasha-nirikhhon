namespace AirlineMatrix
{
    partial class ALMatrix
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
            this.textBox_excel_path = new System.Windows.Forms.TextBox();
            this.label_excel = new System.Windows.Forms.Label();
            this.folderBrowserDialog = new System.Windows.Forms.FolderBrowserDialog();
            this.textBox_row_begin = new System.Windows.Forms.TextBox();
            this.textBox_col_begin = new System.Windows.Forms.TextBox();
            this.textBox_date = new System.Windows.Forms.TextBox();
            this.label_row = new System.Windows.Forms.Label();
            this.label_column = new System.Windows.Forms.Label();
            this.label_date = new System.Windows.Forms.Label();
            this.label_eg = new System.Windows.Forms.Label();
            this.button_OK = new System.Windows.Forms.Button();
            this.button_browse = new System.Windows.Forms.Button();
            this.label1 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.textBox_row_end = new System.Windows.Forms.TextBox();
            this.textBox_col_end = new System.Windows.Forms.TextBox();
            this.SuspendLayout();
            // 
            // textBox_excel_path
            // 
            this.textBox_excel_path.Location = new System.Drawing.Point(145, 81);
            this.textBox_excel_path.Name = "textBox_excel_path";
            this.textBox_excel_path.Size = new System.Drawing.Size(455, 22);
            this.textBox_excel_path.TabIndex = 0;
            // 
            // label_excel
            // 
            this.label_excel.Location = new System.Drawing.Point(51, 81);
            this.label_excel.Name = "label_excel";
            this.label_excel.Size = new System.Drawing.Size(78, 20);
            this.label_excel.TabIndex = 1;
            this.label_excel.Text = "Excel File:";
            // 
            // textBox_row_begin
            // 
            this.textBox_row_begin.Location = new System.Drawing.Point(145, 180);
            this.textBox_row_begin.Name = "textBox_row_begin";
            this.textBox_row_begin.Size = new System.Drawing.Size(58, 22);
            this.textBox_row_begin.TabIndex = 2;
            // 
            // textBox_col_begin
            // 
            this.textBox_col_begin.Location = new System.Drawing.Point(145, 233);
            this.textBox_col_begin.Name = "textBox_col_begin";
            this.textBox_col_begin.Size = new System.Drawing.Size(58, 22);
            this.textBox_col_begin.TabIndex = 3;
            // 
            // textBox_date
            // 
            this.textBox_date.Location = new System.Drawing.Point(446, 179);
            this.textBox_date.Name = "textBox_date";
            this.textBox_date.Size = new System.Drawing.Size(100, 22);
            this.textBox_date.TabIndex = 4;
            // 
            // label_row
            // 
            this.label_row.AutoSize = true;
            this.label_row.Location = new System.Drawing.Point(23, 185);
            this.label_row.Name = "label_row";
            this.label_row.Size = new System.Drawing.Size(111, 17);
            this.label_row.TabIndex = 5;
            this.label_row.Text = "Excel Rows from";
            // 
            // label_column
            // 
            this.label_column.AutoSize = true;
            this.label_column.Location = new System.Drawing.Point(10, 236);
            this.label_column.Name = "label_column";
            this.label_column.Size = new System.Drawing.Size(124, 17);
            this.label_column.TabIndex = 6;
            this.label_column.Text = "Excel Column from";
            // 
            // label_date
            // 
            this.label_date.AutoSize = true;
            this.label_date.Location = new System.Drawing.Point(359, 180);
            this.label_date.Name = "label_date";
            this.label_date.Size = new System.Drawing.Size(81, 17);
            this.label_date.TabIndex = 7;
            this.label_date.Text = "Check Date";
            // 
            // label_eg
            // 
            this.label_eg.AutoSize = true;
            this.label_eg.Location = new System.Drawing.Point(552, 179);
            this.label_eg.Name = "label_eg";
            this.label_eg.Size = new System.Drawing.Size(66, 17);
            this.label_eg.TabIndex = 8;
            this.label_eg.Text = "xxxx-xx-xx";
            // 
            // button_OK
            // 
            this.button_OK.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.button_OK.Location = new System.Drawing.Point(292, 333);
            this.button_OK.Name = "button_OK";
            this.button_OK.Size = new System.Drawing.Size(148, 44);
            this.button_OK.TabIndex = 9;
            this.button_OK.Text = "Confirm and Start";
            this.button_OK.UseVisualStyleBackColor = true;
            this.button_OK.Click += new System.EventHandler(this.button_OK_Click);
            // 
            // button_browse
            // 
            this.button_browse.Location = new System.Drawing.Point(626, 79);
            this.button_browse.Name = "button_browse";
            this.button_browse.Size = new System.Drawing.Size(75, 23);
            this.button_browse.TabIndex = 10;
            this.button_browse.Text = "browse";
            this.button_browse.UseVisualStyleBackColor = true;
            this.button_browse.Click += new System.EventHandler(this.button_browse_Click);
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(210, 184);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(20, 17);
            this.label1.TabIndex = 11;
            this.label1.Text = "to";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(210, 238);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(20, 17);
            this.label2.TabIndex = 12;
            this.label2.Text = "to";
            // 
            // textBox_row_end
            // 
            this.textBox_row_end.Location = new System.Drawing.Point(236, 180);
            this.textBox_row_end.Name = "textBox_row_end";
            this.textBox_row_end.Size = new System.Drawing.Size(58, 22);
            this.textBox_row_end.TabIndex = 13;
            // 
            // textBox_col_end
            // 
            this.textBox_col_end.Location = new System.Drawing.Point(236, 234);
            this.textBox_col_end.Name = "textBox_col_end";
            this.textBox_col_end.Size = new System.Drawing.Size(58, 22);
            this.textBox_col_end.TabIndex = 14;
            // 
            // ALMatrix
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 450);
            this.Controls.Add(this.textBox_col_end);
            this.Controls.Add(this.textBox_row_end);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.button_browse);
            this.Controls.Add(this.button_OK);
            this.Controls.Add(this.label_eg);
            this.Controls.Add(this.label_date);
            this.Controls.Add(this.label_column);
            this.Controls.Add(this.label_row);
            this.Controls.Add(this.textBox_date);
            this.Controls.Add(this.textBox_col_begin);
            this.Controls.Add(this.textBox_row_begin);
            this.Controls.Add(this.label_excel);
            this.Controls.Add(this.textBox_excel_path);
            this.Name = "ALMatrix";
            this.Text = "AL-Matrix";
            this.Load += new System.EventHandler(this.ALMatrix_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox textBox_excel_path;
        private System.Windows.Forms.Label label_excel;
        private System.Windows.Forms.FolderBrowserDialog folderBrowserDialog;
        private System.Windows.Forms.TextBox textBox_row_begin;
        private System.Windows.Forms.TextBox textBox_col_begin;
        private System.Windows.Forms.TextBox textBox_date;
        private System.Windows.Forms.Label label_row;
        private System.Windows.Forms.Label label_column;
        private System.Windows.Forms.Label label_date;
        private System.Windows.Forms.Label label_eg;
        private System.Windows.Forms.Button button_OK;
        private System.Windows.Forms.Button button_browse;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.TextBox textBox_row_end;
        private System.Windows.Forms.TextBox textBox_col_end;
    }
}

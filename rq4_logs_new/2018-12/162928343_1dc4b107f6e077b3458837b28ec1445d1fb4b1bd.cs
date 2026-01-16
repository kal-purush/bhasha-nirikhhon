namespace Task1
{
    partial class MainForm
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
            this.tableLayoutPanel1 = new System.Windows.Forms.TableLayoutPanel();
            this.tbInputFile = new System.Windows.Forms.TextBox();
            this.tbOutput = new System.Windows.Forms.TextBox();
            this.bInputFile = new System.Windows.Forms.Button();
            this.bOutputFile = new System.Windows.Forms.Button();
            this.bRun = new System.Windows.Forms.Button();
            this.openFileDialog = new System.Windows.Forms.OpenFileDialog();
            this.tableLayoutPanel1.SuspendLayout();
            this.SuspendLayout();
            // 
            // tableLayoutPanel1
            // 
            this.tableLayoutPanel1.ColumnCount = 2;
            this.tableLayoutPanel1.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel1.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Absolute, 120F));
            this.tableLayoutPanel1.Controls.Add(this.tbInputFile, 0, 0);
            this.tableLayoutPanel1.Controls.Add(this.tbOutput, 0, 1);
            this.tableLayoutPanel1.Controls.Add(this.bInputFile, 1, 0);
            this.tableLayoutPanel1.Controls.Add(this.bOutputFile, 1, 1);
            this.tableLayoutPanel1.Controls.Add(this.bRun, 1, 2);
            this.tableLayoutPanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel1.Location = new System.Drawing.Point(0, 0);
            this.tableLayoutPanel1.Name = "tableLayoutPanel1";
            this.tableLayoutPanel1.RowCount = 4;
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Absolute, 30F));
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Absolute, 30F));
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Absolute, 30F));
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel1.Size = new System.Drawing.Size(488, 138);
            this.tableLayoutPanel1.TabIndex = 0;
            // 
            // tbInputFile
            // 
            this.tbInputFile.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tbInputFile.Location = new System.Drawing.Point(3, 3);
            this.tbInputFile.Name = "tbInputFile";
            this.tbInputFile.ReadOnly = true;
            this.tbInputFile.Size = new System.Drawing.Size(362, 20);
            this.tbInputFile.TabIndex = 0;
            // 
            // tbOutput
            // 
            this.tbOutput.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tbOutput.Location = new System.Drawing.Point(3, 33);
            this.tbOutput.Name = "tbOutput";
            this.tbOutput.ReadOnly = true;
            this.tbOutput.Size = new System.Drawing.Size(362, 20);
            this.tbOutput.TabIndex = 1;
            // 
            // bInputFile
            // 
            this.bInputFile.Dock = System.Windows.Forms.DockStyle.Fill;
            this.bInputFile.Location = new System.Drawing.Point(371, 3);
            this.bInputFile.Name = "bInputFile";
            this.bInputFile.Size = new System.Drawing.Size(114, 24);
            this.bInputFile.TabIndex = 2;
            this.bInputFile.Text = "Входной файл";
            this.bInputFile.UseVisualStyleBackColor = true;
            this.bInputFile.Click += new System.EventHandler(this.bInputFile_Click);
            // 
            // bOutputFile
            // 
            this.bOutputFile.Dock = System.Windows.Forms.DockStyle.Fill;
            this.bOutputFile.Location = new System.Drawing.Point(371, 33);
            this.bOutputFile.Name = "bOutputFile";
            this.bOutputFile.Size = new System.Drawing.Size(114, 24);
            this.bOutputFile.TabIndex = 3;
            this.bOutputFile.Text = "Выходной файл";
            this.bOutputFile.UseVisualStyleBackColor = true;
            this.bOutputFile.Click += new System.EventHandler(this.bOutputFile_Click);
            // 
            // bRun
            // 
            this.bRun.Dock = System.Windows.Forms.DockStyle.Fill;
            this.bRun.Enabled = false;
            this.bRun.Location = new System.Drawing.Point(371, 63);
            this.bRun.Name = "bRun";
            this.bRun.Size = new System.Drawing.Size(114, 24);
            this.bRun.TabIndex = 4;
            this.bRun.Text = "Подсчитать слова";
            this.bRun.UseVisualStyleBackColor = true;
            this.bRun.Click += new System.EventHandler(this.bRun_Click);
            // 
            // openFileDialog
            // 
            this.openFileDialog.FileName = "openFileDialog1";
            // 
            // MainForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(488, 138);
            this.Controls.Add(this.tableLayoutPanel1);
            this.MinimumSize = new System.Drawing.Size(504, 177);
            this.Name = "MainForm";
            this.Text = "Task1";
            this.tableLayoutPanel1.ResumeLayout(false);
            this.tableLayoutPanel1.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.TableLayoutPanel tableLayoutPanel1;
        private System.Windows.Forms.TextBox tbInputFile;
        private System.Windows.Forms.TextBox tbOutput;
        private System.Windows.Forms.Button bInputFile;
        private System.Windows.Forms.Button bOutputFile;
        private System.Windows.Forms.Button bRun;
        private System.Windows.Forms.OpenFileDialog openFileDialog;
    }
}

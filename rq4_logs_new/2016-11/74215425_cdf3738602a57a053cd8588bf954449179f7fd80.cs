namespace server
{
    partial class serverForm
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
            this.Label_Header = new System.Windows.Forms.Label();
            this.Label_Path = new System.Windows.Forms.Label();
            this.Label_Port = new System.Windows.Forms.Label();
            this.Header_Logs = new System.Windows.Forms.Label();
            this.RTextBox_Logs = new System.Windows.Forms.RichTextBox();
            this.serverPath = new System.Windows.Forms.TextBox();
            this.Numeric_Port = new System.Windows.Forms.NumericUpDown();
            this.Button_Start = new System.Windows.Forms.Button();
            this.Button_Stop = new System.Windows.Forms.Button();
            this.serverBrowse = new System.Windows.Forms.Button();
            ((System.ComponentModel.ISupportInitialize)(this.Numeric_Port)).BeginInit();
            this.SuspendLayout();
            // 
            // Label_Header
            // 
            this.Label_Header.AutoSize = true;
            this.Label_Header.Font = new System.Drawing.Font("Microsoft Sans Serif", 15.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(162)));
            this.Label_Header.Location = new System.Drawing.Point(215, 11);
            this.Label_Header.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.Label_Header.Name = "Label_Header";
            this.Label_Header.Size = new System.Drawing.Size(325, 31);
            this.Label_Header.TabIndex = 0;
            this.Label_Header.Text = "Cloud File Storage Server";
            // 
            // Label_Path
            // 
            this.Label_Path.AutoSize = true;
            this.Label_Path.Location = new System.Drawing.Point(61, 121);
            this.Label_Path.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.Label_Path.Name = "Label_Path";
            this.Label_Path.Size = new System.Drawing.Size(41, 17);
            this.Label_Path.TabIndex = 1;
            this.Label_Path.Text = "Path:";
            // 
            // Label_Port
            // 
            this.Label_Port.AutoSize = true;
            this.Label_Port.Location = new System.Drawing.Point(61, 73);
            this.Label_Port.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.Label_Port.Name = "Label_Port";
            this.Label_Port.Size = new System.Drawing.Size(38, 17);
            this.Label_Port.TabIndex = 2;
            this.Label_Port.Text = "Port:";
            // 
            // Header_Logs
            // 
            this.Header_Logs.AutoSize = true;
            this.Header_Logs.Location = new System.Drawing.Point(61, 207);
            this.Header_Logs.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.Header_Logs.Name = "Header_Logs";
            this.Header_Logs.Size = new System.Drawing.Size(43, 17);
            this.Header_Logs.TabIndex = 3;
            this.Header_Logs.Text = "Logs:";
            // 
            // RTextBox_Logs
            // 
            this.RTextBox_Logs.Location = new System.Drawing.Point(132, 207);
            this.RTextBox_Logs.Margin = new System.Windows.Forms.Padding(4);
            this.RTextBox_Logs.Name = "RTextBox_Logs";
            this.RTextBox_Logs.Size = new System.Drawing.Size(550, 189);
            this.RTextBox_Logs.TabIndex = 4;
            this.RTextBox_Logs.Text = "";
            // 
            // serverPath
            // 
            this.serverPath.Location = new System.Drawing.Point(132, 117);
            this.serverPath.Margin = new System.Windows.Forms.Padding(4);
            this.serverPath.Name = "serverPath";
            this.serverPath.Size = new System.Drawing.Size(239, 22);
            this.serverPath.TabIndex = 6;
            // 
            // Numeric_Port
            // 
            this.Numeric_Port.Location = new System.Drawing.Point(132, 70);
            this.Numeric_Port.Margin = new System.Windows.Forms.Padding(4);
            this.Numeric_Port.Maximum = new decimal(new int[] {
            100000,
            0,
            0,
            0});
            this.Numeric_Port.Name = "Numeric_Port";
            this.Numeric_Port.Size = new System.Drawing.Size(239, 22);
            this.Numeric_Port.TabIndex = 7;
            // 
            // Button_Start
            // 
            this.Button_Start.Location = new System.Drawing.Point(423, 73);
            this.Button_Start.Margin = new System.Windows.Forms.Padding(4);
            this.Button_Start.Name = "Button_Start";
            this.Button_Start.Size = new System.Drawing.Size(125, 64);
            this.Button_Start.TabIndex = 8;
            this.Button_Start.Text = "Start Listening";
            this.Button_Start.UseVisualStyleBackColor = true;
            this.Button_Start.Click += new System.EventHandler(this.Button_Start_Click);
            // 
            // Button_Stop
            // 
            this.Button_Stop.Location = new System.Drawing.Point(577, 73);
            this.Button_Stop.Margin = new System.Windows.Forms.Padding(4);
            this.Button_Stop.Name = "Button_Stop";
            this.Button_Stop.Size = new System.Drawing.Size(125, 64);
            this.Button_Stop.TabIndex = 9;
            this.Button_Stop.Text = "Stop Listening";
            this.Button_Stop.UseVisualStyleBackColor = true;
            this.Button_Stop.Click += new System.EventHandler(this.Button_Stop_Click);
            // 
            // serverBrowse
            // 
            this.serverBrowse.Location = new System.Drawing.Point(296, 160);
            this.serverBrowse.Name = "serverBrowse";
            this.serverBrowse.Size = new System.Drawing.Size(75, 23);
            this.serverBrowse.TabIndex = 10;
            this.serverBrowse.Text = "Browse";
            this.serverBrowse.UseVisualStyleBackColor = true;
            this.serverBrowse.Click += new System.EventHandler(this.serverBrowse_Click);
            // 
            // serverForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(748, 411);
            this.Controls.Add(this.serverBrowse);
            this.Controls.Add(this.Button_Stop);
            this.Controls.Add(this.Button_Start);
            this.Controls.Add(this.Numeric_Port);
            this.Controls.Add(this.serverPath);
            this.Controls.Add(this.RTextBox_Logs);
            this.Controls.Add(this.Header_Logs);
            this.Controls.Add(this.Label_Port);
            this.Controls.Add(this.Label_Path);
            this.Controls.Add(this.Label_Header);
            this.Margin = new System.Windows.Forms.Padding(4);
            this.Name = "serverForm";
            this.Text = "ServerGUI";
            this.Load += new System.EventHandler(this.serverForm_Load);
            ((System.ComponentModel.ISupportInitialize)(this.Numeric_Port)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label Label_Header;
        private System.Windows.Forms.Label Label_Path;
        private System.Windows.Forms.Label Label_Port;
        private System.Windows.Forms.Label Header_Logs;
        private System.Windows.Forms.RichTextBox RTextBox_Logs;
        private System.Windows.Forms.TextBox serverPath;
        private System.Windows.Forms.NumericUpDown Numeric_Port;
        private System.Windows.Forms.Button Button_Start;
        private System.Windows.Forms.Button Button_Stop;
        private System.Windows.Forms.Button serverBrowse;
    }
}

namespace CSGO_Dedicated_Server
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
            this.label_path = new System.Windows.Forms.Label();
            this.button_chooseFile = new System.Windows.Forms.Button();
            this.openFileDialog1 = new System.Windows.Forms.OpenFileDialog();
            this.comboBox_gamemode = new System.Windows.Forms.ComboBox();
            this.button_start = new System.Windows.Forms.Button();
            this.label_gamemode = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.label4 = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // label_path
            // 
            this.label_path.AutoSize = true;
            this.label_path.Location = new System.Drawing.Point(12, 87);
            this.label_path.Name = "label_path";
            this.label_path.Size = new System.Drawing.Size(32, 13);
            this.label_path.TabIndex = 0;
            this.label_path.Text = "Path:";
            // 
            // button_chooseFile
            // 
            this.button_chooseFile.Location = new System.Drawing.Point(15, 103);
            this.button_chooseFile.Name = "button_chooseFile";
            this.button_chooseFile.Size = new System.Drawing.Size(101, 23);
            this.button_chooseFile.TabIndex = 2;
            this.button_chooseFile.Text = "Change path...";
            this.button_chooseFile.UseVisualStyleBackColor = true;
            this.button_chooseFile.Click += new System.EventHandler(this.button_chooseFile_Click);
            // 
            // openFileDialog1
            // 
            this.openFileDialog1.FileName = "openFileDialog1";
            this.openFileDialog1.Title = "Locate \'srcds.exe\' path";
            // 
            // comboBox_gamemode
            // 
            this.comboBox_gamemode.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.comboBox_gamemode.FormattingEnabled = true;
            this.comboBox_gamemode.Items.AddRange(new object[] {
            "Casual",
            "Competitive",
            "Arms Race",
            "Demolition",
            "Deathmatch"});
            this.comboBox_gamemode.Location = new System.Drawing.Point(15, 175);
            this.comboBox_gamemode.Name = "comboBox_gamemode";
            this.comboBox_gamemode.Size = new System.Drawing.Size(121, 21);
            this.comboBox_gamemode.TabIndex = 3;
            this.comboBox_gamemode.SelectedIndexChanged += new System.EventHandler(this.comboBox_gamemode_SelectedIndexChanged);
            // 
            // button_start
            // 
            this.button_start.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.button_start.Location = new System.Drawing.Point(15, 234);
            this.button_start.Name = "button_start";
            this.button_start.Size = new System.Drawing.Size(93, 39);
            this.button_start.TabIndex = 4;
            this.button_start.Text = "Start";
            this.button_start.UseVisualStyleBackColor = true;
            this.button_start.Click += new System.EventHandler(this.button_start_Click);
            // 
            // label_gamemode
            // 
            this.label_gamemode.AutoSize = true;
            this.label_gamemode.Location = new System.Drawing.Point(12, 159);
            this.label_gamemode.Name = "label_gamemode";
            this.label_gamemode.Size = new System.Drawing.Size(64, 13);
            this.label_gamemode.TabIndex = 5;
            this.label_gamemode.Text = "Game mode";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(12, 29);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(414, 13);
            this.label1.TabIndex = 6;
            this.label1.Text = "1. Choose the csgo dedicated server path. Example: \"C:\\steamcmd\\cs_go\\srcds.exe\"";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(12, 42);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(107, 13);
            this.label2.TabIndex = 7;
            this.label2.Text = "2. Select game mode";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(12, 55);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(119, 13);
            this.label3.TabIndex = 8;
            this.label3.Text = "3. Press the start button";
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(12, 9);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(64, 13);
            this.label4.TabIndex = 9;
            this.label4.Text = "How to use:";
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(435, 345);
            this.Controls.Add(this.label4);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.label_gamemode);
            this.Controls.Add(this.button_start);
            this.Controls.Add(this.comboBox_gamemode);
            this.Controls.Add(this.button_chooseFile);
            this.Controls.Add(this.label_path);
            this.Name = "Form1";
            this.Text = "CSGO Dedicated launcher";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label_path;
        private System.Windows.Forms.Button button_chooseFile;
        private System.Windows.Forms.OpenFileDialog openFileDialog1;
        private System.Windows.Forms.ComboBox comboBox_gamemode;
        private System.Windows.Forms.Button button_start;
        private System.Windows.Forms.Label label_gamemode;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.Label label4;
    }
}

namespace ComPort
{
    partial class ComPort
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
            this.components = new System.ComponentModel.Container();
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(ComPort));
            this.serialPort = new System.IO.Ports.SerialPort(this.components);
            this.comPort_status = new System.Windows.Forms.PictureBox();
            this.vid_textBox = new System.Windows.Forms.TextBox();
            this.pid_textBox = new System.Windows.Forms.TextBox();
            this.vid_label = new System.Windows.Forms.Label();
            this.pid_label = new System.Windows.Forms.Label();
            this.ComPortLabel = new System.Windows.Forms.Label();
            this.send_tbox = new System.Windows.Forms.TextBox();
            this.send_btn = new System.Windows.Forms.Button();
            ((System.ComponentModel.ISupportInitialize)(this.comPort_status)).BeginInit();
            this.SuspendLayout();
            // 
            // serialPort
            // 
            this.serialPort.BaudRate = 115200;
            // 
            // comPort_status
            // 
            this.comPort_status.Image = ((System.Drawing.Image)(resources.GetObject("comPort_status.Image")));
            this.comPort_status.Location = new System.Drawing.Point(23, 74);
            this.comPort_status.Name = "comPort_status";
            this.comPort_status.Size = new System.Drawing.Size(180, 163);
            this.comPort_status.SizeMode = System.Windows.Forms.PictureBoxSizeMode.Zoom;
            this.comPort_status.TabIndex = 0;
            this.comPort_status.TabStop = false;
            // 
            // vid_textBox
            // 
            this.vid_textBox.Location = new System.Drawing.Point(280, 74);
            this.vid_textBox.Name = "vid_textBox";
            this.vid_textBox.Size = new System.Drawing.Size(100, 20);
            this.vid_textBox.TabIndex = 0;
            // 
            // pid_textBox
            // 
            this.pid_textBox.Location = new System.Drawing.Point(280, 119);
            this.pid_textBox.Name = "pid_textBox";
            this.pid_textBox.Size = new System.Drawing.Size(100, 20);
            this.pid_textBox.TabIndex = 1;
            // 
            // vid_label
            // 
            this.vid_label.AutoSize = true;
            this.vid_label.Location = new System.Drawing.Point(230, 77);
            this.vid_label.Name = "vid_label";
            this.vid_label.Size = new System.Drawing.Size(25, 13);
            this.vid_label.TabIndex = 2;
            this.vid_label.Text = "VID";
            // 
            // pid_label
            // 
            this.pid_label.AutoSize = true;
            this.pid_label.Location = new System.Drawing.Point(230, 122);
            this.pid_label.Name = "pid_label";
            this.pid_label.Size = new System.Drawing.Size(25, 13);
            this.pid_label.TabIndex = 2;
            this.pid_label.Text = "PID";
            // 
            // ComPortLabel
            // 
            this.ComPortLabel.AutoSize = true;
            this.ComPortLabel.Enabled = false;
            this.ComPortLabel.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.ComPortLabel.Location = new System.Drawing.Point(277, 154);
            this.ComPortLabel.Name = "ComPortLabel";
            this.ComPortLabel.Size = new System.Drawing.Size(72, 13);
            this.ComPortLabel.TabIndex = 3;
            this.ComPortLabel.Text = "COM PORT";
            // 
            // send_tbox
            // 
            this.send_tbox.Location = new System.Drawing.Point(233, 180);
            this.send_tbox.Name = "send_tbox";
            this.send_tbox.Size = new System.Drawing.Size(147, 20);
            this.send_tbox.TabIndex = 4;
            // 
            // send_btn
            // 
            this.send_btn.Location = new System.Drawing.Point(233, 214);
            this.send_btn.Name = "send_btn";
            this.send_btn.Size = new System.Drawing.Size(75, 23);
            this.send_btn.TabIndex = 5;
            this.send_btn.Text = "Send Data";
            this.send_btn.UseVisualStyleBackColor = true;
            this.send_btn.Click += new System.EventHandler(this.send_btn_Click);
            // 
            // ComPort
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("$this.BackgroundImage")));
            this.BorderStyle = MetroFramework.Drawing.MetroBorderStyle.FixedSingle;
            this.ClientSize = new System.Drawing.Size(403, 261);
            this.Controls.Add(this.send_btn);
            this.Controls.Add(this.send_tbox);
            this.Controls.Add(this.ComPortLabel);
            this.Controls.Add(this.pid_label);
            this.Controls.Add(this.vid_label);
            this.Controls.Add(this.pid_textBox);
            this.Controls.Add(this.vid_textBox);
            this.Controls.Add(this.comPort_status);
            this.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Name = "ComPort";
            this.Text = "Virtual Com Port";
            ((System.ComponentModel.ISupportInitialize)(this.comPort_status)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.IO.Ports.SerialPort serialPort;
        private System.Windows.Forms.PictureBox comPort_status;
        private System.Windows.Forms.TextBox vid_textBox;
        private System.Windows.Forms.TextBox pid_textBox;
        private System.Windows.Forms.Label vid_label;
        private System.Windows.Forms.Label pid_label;
        private System.Windows.Forms.Label ComPortLabel;
        private System.Windows.Forms.TextBox send_tbox;
        private System.Windows.Forms.Button send_btn;
    }
}

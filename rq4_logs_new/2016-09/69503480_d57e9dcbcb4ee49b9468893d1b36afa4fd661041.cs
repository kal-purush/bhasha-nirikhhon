namespace CpuPcStack
{
    partial class FrmMain
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
            this.label_ip = new System.Windows.Forms.Label();
            this.groupBox_client = new System.Windows.Forms.GroupBox();
            this.textBox_remotePort = new System.Windows.Forms.TextBox();
            this.label_port = new System.Windows.Forms.Label();
            this.textBox_remote_ip = new System.Windows.Forms.TextBox();
            this.checkBox_start_server = new System.Windows.Forms.CheckBox();
            this.label_srv_port = new System.Windows.Forms.Label();
            this.textBox_srv_port = new System.Windows.Forms.TextBox();
            this.textBox_frame_msg = new System.Windows.Forms.TextBox();
            this.textBox_send = new System.Windows.Forms.TextBox();
            this.label_msg = new System.Windows.Forms.Label();
            this.button_send_request = new System.Windows.Forms.Button();
            this.groupBox_listener = new System.Windows.Forms.GroupBox();
            this.label_payload_ascii = new System.Windows.Forms.Label();
            this.label_payload_byte = new System.Windows.Forms.Label();
            this.textBox_msg_payload_ASCII = new System.Windows.Forms.TextBox();
            this.textBox_msg_payload = new System.Windows.Forms.TextBox();
            this.checkBox_receive_big_endian = new System.Windows.Forms.CheckBox();
            this.checkBox_send_big_endian = new System.Windows.Forms.CheckBox();
            this.comboBox_frame_type = new System.Windows.Forms.ComboBox();
            this.comboBox_local_ips = new System.Windows.Forms.ComboBox();
            this.label_host_name = new System.Windows.Forms.Label();
            this.groupBox_local_adress = new System.Windows.Forms.GroupBox();
            this.label_host_name_desc = new System.Windows.Forms.Label();
            this.textBox_send_index = new System.Windows.Forms.TextBox();
            this.groupBox_send_msg = new System.Windows.Forms.GroupBox();
            this.button_send_repeat = new System.Windows.Forms.Button();
            this.label1 = new System.Windows.Forms.Label();
            this.label_send_times = new System.Windows.Forms.Label();
            this.button_check = new System.Windows.Forms.Button();
            this.label_cyclic_desc = new System.Windows.Forms.Label();
            this.checkBox_cyclic = new System.Windows.Forms.CheckBox();
            this.textBox_timer_interval = new System.Windows.Forms.TextBox();
            this.label_repeat = new System.Windows.Forms.Label();
            this.radioButton_send_ascii = new System.Windows.Forms.RadioButton();
            this.radioButton_send_byte = new System.Windows.Forms.RadioButton();
            this.textBox_send_multiplikator = new System.Windows.Forms.TextBox();
            this.groupBox_status = new System.Windows.Forms.GroupBox();
            this.richTextBox_fstack = new System.Windows.Forms.RichTextBox();
            this.richTextBox_fstackLog = new System.Windows.Forms.RichTextBox();
            this.menuStrip_main = new System.Windows.Forms.MenuStrip();
            this.resetToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.logFileToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.groupBox_client.SuspendLayout();
            this.groupBox_listener.SuspendLayout();
            this.groupBox_local_adress.SuspendLayout();
            this.groupBox_send_msg.SuspendLayout();
            this.groupBox_status.SuspendLayout();
            this.menuStrip_main.SuspendLayout();
            this.SuspendLayout();
            // 
            // label_ip
            // 
            this.label_ip.AutoSize = true;
            this.label_ip.Location = new System.Drawing.Point(6, 16);
            this.label_ip.Name = "label_ip";
            this.label_ip.Size = new System.Drawing.Size(51, 13);
            this.label_ip.TabIndex = 0;
            this.label_ip.Text = "IPadress:";
            // 
            // groupBox_client
            // 
            this.groupBox_client.Controls.Add(this.textBox_remotePort);
            this.groupBox_client.Controls.Add(this.label_port);
            this.groupBox_client.Controls.Add(this.textBox_remote_ip);
            this.groupBox_client.Controls.Add(this.label_ip);
            this.groupBox_client.Location = new System.Drawing.Point(9, 321);
            this.groupBox_client.Name = "groupBox_client";
            this.groupBox_client.Size = new System.Drawing.Size(174, 72);
            this.groupBox_client.TabIndex = 1;
            this.groupBox_client.TabStop = false;
            this.groupBox_client.Text = "remote";
            // 
            // textBox_remotePort
            // 
            this.textBox_remotePort.Location = new System.Drawing.Point(82, 39);
            this.textBox_remotePort.Name = "textBox_remotePort";
            this.textBox_remotePort.Size = new System.Drawing.Size(81, 20);
            this.textBox_remotePort.TabIndex = 3;
            this.textBox_remotePort.Text = "2202";
            // 
            // label_port
            // 
            this.label_port.AutoSize = true;
            this.label_port.Location = new System.Drawing.Point(6, 42);
            this.label_port.Name = "label_port";
            this.label_port.Size = new System.Drawing.Size(28, 13);
            this.label_port.TabIndex = 2;
            this.label_port.Text = "port:";
            // 
            // textBox_remote_ip
            // 
            this.textBox_remote_ip.Location = new System.Drawing.Point(82, 13);
            this.textBox_remote_ip.Name = "textBox_remote_ip";
            this.textBox_remote_ip.Size = new System.Drawing.Size(81, 20);
            this.textBox_remote_ip.TabIndex = 1;
            this.textBox_remote_ip.Text = "192.168.1.205";
            // 
            // checkBox_start_server
            // 
            this.checkBox_start_server.AutoSize = true;
            this.checkBox_start_server.Location = new System.Drawing.Point(12, 21);
            this.checkBox_start_server.Name = "checkBox_start_server";
            this.checkBox_start_server.Size = new System.Drawing.Size(78, 17);
            this.checkBox_start_server.TabIndex = 3;
            this.checkBox_start_server.Text = "start server";
            this.checkBox_start_server.UseVisualStyleBackColor = true;
            this.checkBox_start_server.CheckedChanged += new System.EventHandler(this.checkBox_start_server_CheckedChanged);
            // 
            // label_srv_port
            // 
            this.label_srv_port.AutoSize = true;
            this.label_srv_port.Location = new System.Drawing.Point(119, 22);
            this.label_srv_port.Name = "label_srv_port";
            this.label_srv_port.Size = new System.Drawing.Size(66, 13);
            this.label_srv_port.TabIndex = 5;
            this.label_srv_port.Text = "listen @port:";
            // 
            // textBox_srv_port
            // 
            this.textBox_srv_port.Location = new System.Drawing.Point(191, 19);
            this.textBox_srv_port.Name = "textBox_srv_port";
            this.textBox_srv_port.Size = new System.Drawing.Size(44, 20);
            this.textBox_srv_port.TabIndex = 6;
            this.textBox_srv_port.Text = "50000";
            // 
            // textBox_frame_msg
            // 
            this.textBox_frame_msg.Location = new System.Drawing.Point(89, 48);
            this.textBox_frame_msg.Name = "textBox_frame_msg";
            this.textBox_frame_msg.Size = new System.Drawing.Size(192, 20);
            this.textBox_frame_msg.TabIndex = 8;
            // 
            // textBox_send
            // 
            this.textBox_send.Location = new System.Drawing.Point(161, 23);
            this.textBox_send.Name = "textBox_send";
            this.textBox_send.Size = new System.Drawing.Size(140, 20);
            this.textBox_send.TabIndex = 9;
            this.textBox_send.Text = "1,2,3,4";
            // 
            // label_msg
            // 
            this.label_msg.AutoSize = true;
            this.label_msg.Location = new System.Drawing.Point(12, 52);
            this.label_msg.Name = "label_msg";
            this.label_msg.Size = new System.Drawing.Size(71, 13);
            this.label_msg.TabIndex = 10;
            this.label_msg.Text = "last message:";
            // 
            // button_send_request
            // 
            this.button_send_request.Location = new System.Drawing.Point(9, 50);
            this.button_send_request.Name = "button_send_request";
            this.button_send_request.Size = new System.Drawing.Size(292, 23);
            this.button_send_request.TabIndex = 11;
            this.button_send_request.Text = "send";
            this.button_send_request.UseVisualStyleBackColor = true;
            this.button_send_request.Click += new System.EventHandler(this.button_send_request_Click);
            // 
            // groupBox_listener
            // 
            this.groupBox_listener.Controls.Add(this.label_payload_ascii);
            this.groupBox_listener.Controls.Add(this.label_payload_byte);
            this.groupBox_listener.Controls.Add(this.textBox_msg_payload_ASCII);
            this.groupBox_listener.Controls.Add(this.textBox_msg_payload);
            this.groupBox_listener.Controls.Add(this.textBox_frame_msg);
            this.groupBox_listener.Controls.Add(this.label_msg);
            this.groupBox_listener.Controls.Add(this.checkBox_start_server);
            this.groupBox_listener.Controls.Add(this.label_srv_port);
            this.groupBox_listener.Controls.Add(this.textBox_srv_port);
            this.groupBox_listener.Location = new System.Drawing.Point(12, 27);
            this.groupBox_listener.Name = "groupBox_listener";
            this.groupBox_listener.Size = new System.Drawing.Size(313, 134);
            this.groupBox_listener.TabIndex = 12;
            this.groupBox_listener.TabStop = false;
            this.groupBox_listener.Text = "listener";
            // 
            // label_payload_ascii
            // 
            this.label_payload_ascii.AutoSize = true;
            this.label_payload_ascii.Location = new System.Drawing.Point(12, 103);
            this.label_payload_ascii.Name = "label_payload_ascii";
            this.label_payload_ascii.Size = new System.Drawing.Size(71, 13);
            this.label_payload_ascii.TabIndex = 15;
            this.label_payload_ascii.Text = "payload ascii:";
            // 
            // label_payload_byte
            // 
            this.label_payload_byte.AutoSize = true;
            this.label_payload_byte.Location = new System.Drawing.Point(12, 77);
            this.label_payload_byte.Name = "label_payload_byte";
            this.label_payload_byte.Size = new System.Drawing.Size(70, 13);
            this.label_payload_byte.TabIndex = 14;
            this.label_payload_byte.Text = "payload byte:";
            // 
            // textBox_msg_payload_ASCII
            // 
            this.textBox_msg_payload_ASCII.Location = new System.Drawing.Point(89, 100);
            this.textBox_msg_payload_ASCII.Name = "textBox_msg_payload_ASCII";
            this.textBox_msg_payload_ASCII.Size = new System.Drawing.Size(192, 20);
            this.textBox_msg_payload_ASCII.TabIndex = 13;
            // 
            // textBox_msg_payload
            // 
            this.textBox_msg_payload.Location = new System.Drawing.Point(89, 74);
            this.textBox_msg_payload.Name = "textBox_msg_payload";
            this.textBox_msg_payload.Size = new System.Drawing.Size(192, 20);
            this.textBox_msg_payload.TabIndex = 12;
            // 
            // checkBox_receive_big_endian
            // 
            this.checkBox_receive_big_endian.AutoSize = true;
            this.checkBox_receive_big_endian.Location = new System.Drawing.Point(9, 120);
            this.checkBox_receive_big_endian.Name = "checkBox_receive_big_endian";
            this.checkBox_receive_big_endian.Size = new System.Drawing.Size(112, 17);
            this.checkBox_receive_big_endian.TabIndex = 12;
            this.checkBox_receive_big_endian.Text = "receive BigEndian";
            this.checkBox_receive_big_endian.UseVisualStyleBackColor = true;
            this.checkBox_receive_big_endian.CheckedChanged += new System.EventHandler(this.checkBox_receive_big_endian_CheckedChanged);
            // 
            // checkBox_send_big_endian
            // 
            this.checkBox_send_big_endian.AutoSize = true;
            this.checkBox_send_big_endian.Location = new System.Drawing.Point(9, 100);
            this.checkBox_send_big_endian.Name = "checkBox_send_big_endian";
            this.checkBox_send_big_endian.Size = new System.Drawing.Size(100, 17);
            this.checkBox_send_big_endian.TabIndex = 11;
            this.checkBox_send_big_endian.Text = "send BigEndian";
            this.checkBox_send_big_endian.UseVisualStyleBackColor = true;
            this.checkBox_send_big_endian.CheckedChanged += new System.EventHandler(this.checkBox_big_endian_CheckedChanged);
            // 
            // comboBox_frame_type
            // 
            this.comboBox_frame_type.FormattingEnabled = true;
            this.comboBox_frame_type.Items.AddRange(new object[] {
            "TSYN",
            "TCMD"});
            this.comboBox_frame_type.Location = new System.Drawing.Point(9, 23);
            this.comboBox_frame_type.Name = "comboBox_frame_type";
            this.comboBox_frame_type.Size = new System.Drawing.Size(73, 21);
            this.comboBox_frame_type.TabIndex = 13;
            this.comboBox_frame_type.Text = "DEMO";
            // 
            // comboBox_local_ips
            // 
            this.comboBox_local_ips.FormattingEnabled = true;
            this.comboBox_local_ips.Location = new System.Drawing.Point(9, 32);
            this.comboBox_local_ips.Name = "comboBox_local_ips";
            this.comboBox_local_ips.Size = new System.Drawing.Size(153, 21);
            this.comboBox_local_ips.TabIndex = 14;
            this.comboBox_local_ips.SelectedIndexChanged += new System.EventHandler(this.comboBox_local_ips_SelectedIndexChanged);
            // 
            // label_host_name
            // 
            this.label_host_name.AutoSize = true;
            this.label_host_name.Location = new System.Drawing.Point(44, 16);
            this.label_host_name.Name = "label_host_name";
            this.label_host_name.Size = new System.Drawing.Size(56, 13);
            this.label_host_name.TabIndex = 15;
            this.label_host_name.Text = "host name";
            // 
            // groupBox_local_adress
            // 
            this.groupBox_local_adress.Controls.Add(this.label_host_name_desc);
            this.groupBox_local_adress.Controls.Add(this.comboBox_local_ips);
            this.groupBox_local_adress.Controls.Add(this.label_host_name);
            this.groupBox_local_adress.Location = new System.Drawing.Point(9, 247);
            this.groupBox_local_adress.Name = "groupBox_local_adress";
            this.groupBox_local_adress.Size = new System.Drawing.Size(174, 68);
            this.groupBox_local_adress.TabIndex = 16;
            this.groupBox_local_adress.TabStop = false;
            this.groupBox_local_adress.Text = "local";
            // 
            // label_host_name_desc
            // 
            this.label_host_name_desc.AutoSize = true;
            this.label_host_name_desc.Location = new System.Drawing.Point(6, 16);
            this.label_host_name_desc.Name = "label_host_name_desc";
            this.label_host_name_desc.Size = new System.Drawing.Size(32, 13);
            this.label_host_name_desc.TabIndex = 16;
            this.label_host_name_desc.Text = "Host:";
            // 
            // textBox_send_index
            // 
            this.textBox_send_index.Location = new System.Drawing.Point(88, 23);
            this.textBox_send_index.Name = "textBox_send_index";
            this.textBox_send_index.Size = new System.Drawing.Size(67, 20);
            this.textBox_send_index.TabIndex = 17;
            this.textBox_send_index.Text = "1";
            // 
            // groupBox_send_msg
            // 
            this.groupBox_send_msg.Controls.Add(this.button_send_repeat);
            this.groupBox_send_msg.Controls.Add(this.label1);
            this.groupBox_send_msg.Controls.Add(this.button_send_request);
            this.groupBox_send_msg.Controls.Add(this.groupBox_client);
            this.groupBox_send_msg.Controls.Add(this.label_send_times);
            this.groupBox_send_msg.Controls.Add(this.groupBox_local_adress);
            this.groupBox_send_msg.Controls.Add(this.textBox_send);
            this.groupBox_send_msg.Controls.Add(this.button_check);
            this.groupBox_send_msg.Controls.Add(this.label_cyclic_desc);
            this.groupBox_send_msg.Controls.Add(this.comboBox_frame_type);
            this.groupBox_send_msg.Controls.Add(this.checkBox_cyclic);
            this.groupBox_send_msg.Controls.Add(this.textBox_send_index);
            this.groupBox_send_msg.Controls.Add(this.textBox_timer_interval);
            this.groupBox_send_msg.Controls.Add(this.checkBox_send_big_endian);
            this.groupBox_send_msg.Controls.Add(this.label_repeat);
            this.groupBox_send_msg.Controls.Add(this.checkBox_receive_big_endian);
            this.groupBox_send_msg.Controls.Add(this.radioButton_send_ascii);
            this.groupBox_send_msg.Controls.Add(this.radioButton_send_byte);
            this.groupBox_send_msg.Controls.Add(this.textBox_send_multiplikator);
            this.groupBox_send_msg.Location = new System.Drawing.Point(12, 167);
            this.groupBox_send_msg.Name = "groupBox_send_msg";
            this.groupBox_send_msg.Size = new System.Drawing.Size(313, 402);
            this.groupBox_send_msg.TabIndex = 18;
            this.groupBox_send_msg.TabStop = false;
            this.groupBox_send_msg.Text = "send message";
            // 
            // button_send_repeat
            // 
            this.button_send_repeat.Location = new System.Drawing.Point(218, 204);
            this.button_send_repeat.Name = "button_send_repeat";
            this.button_send_repeat.Size = new System.Drawing.Size(75, 23);
            this.button_send_repeat.TabIndex = 28;
            this.button_send_repeat.Text = "send";
            this.button_send_repeat.UseVisualStyleBackColor = true;
            this.button_send_repeat.Click += new System.EventHandler(this.button_send_repeat_Click);
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(189, 247);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(35, 13);
            this.label1.TabIndex = 21;
            this.label1.Text = "label1";
            // 
            // label_send_times
            // 
            this.label_send_times.AutoSize = true;
            this.label_send_times.Location = new System.Drawing.Point(132, 210);
            this.label_send_times.Name = "label_send_times";
            this.label_send_times.Size = new System.Drawing.Size(80, 13);
            this.label_send_times.TabIndex = 27;
            this.label_send_times.Text = "Frames at once";
            // 
            // button_check
            // 
            this.button_check.Location = new System.Drawing.Point(9, 169);
            this.button_check.Name = "button_check";
            this.button_check.Size = new System.Drawing.Size(112, 23);
            this.button_check.TabIndex = 26;
            this.button_check.Text = "check conection";
            this.button_check.UseVisualStyleBackColor = true;
            this.button_check.Click += new System.EventHandler(this.button_check_Click);
            // 
            // label_cyclic_desc
            // 
            this.label_cyclic_desc.AutoSize = true;
            this.label_cyclic_desc.Location = new System.Drawing.Point(287, 149);
            this.label_cyclic_desc.Name = "label_cyclic_desc";
            this.label_cyclic_desc.Size = new System.Drawing.Size(20, 13);
            this.label_cyclic_desc.TabIndex = 25;
            this.label_cyclic_desc.Text = "ms";
            // 
            // checkBox_cyclic
            // 
            this.checkBox_cyclic.AutoSize = true;
            this.checkBox_cyclic.Location = new System.Drawing.Point(164, 148);
            this.checkBox_cyclic.Name = "checkBox_cyclic";
            this.checkBox_cyclic.Size = new System.Drawing.Size(53, 17);
            this.checkBox_cyclic.TabIndex = 24;
            this.checkBox_cyclic.Text = "cyclic";
            this.checkBox_cyclic.UseVisualStyleBackColor = true;
            this.checkBox_cyclic.CheckedChanged += new System.EventHandler(this.checkBox_cyclic_CheckedChanged);
            // 
            // textBox_timer_interval
            // 
            this.textBox_timer_interval.Location = new System.Drawing.Point(237, 146);
            this.textBox_timer_interval.Name = "textBox_timer_interval";
            this.textBox_timer_interval.Size = new System.Drawing.Size(44, 20);
            this.textBox_timer_interval.TabIndex = 23;
            this.textBox_timer_interval.Text = "1000";
            // 
            // label_repeat
            // 
            this.label_repeat.AutoSize = true;
            this.label_repeat.Location = new System.Drawing.Point(9, 210);
            this.label_repeat.Name = "label_repeat";
            this.label_repeat.Size = new System.Drawing.Size(33, 13);
            this.label_repeat.TabIndex = 22;
            this.label_repeat.Text = "send:";
            // 
            // radioButton_send_ascii
            // 
            this.radioButton_send_ascii.AutoSize = true;
            this.radioButton_send_ascii.Location = new System.Drawing.Point(164, 119);
            this.radioButton_send_ascii.Name = "radioButton_send_ascii";
            this.radioButton_send_ascii.Size = new System.Drawing.Size(72, 17);
            this.radioButton_send_ascii.TabIndex = 14;
            this.radioButton_send_ascii.Text = "send ascii";
            this.radioButton_send_ascii.UseVisualStyleBackColor = true;
            // 
            // radioButton_send_byte
            // 
            this.radioButton_send_byte.AutoSize = true;
            this.radioButton_send_byte.Checked = true;
            this.radioButton_send_byte.Location = new System.Drawing.Point(164, 99);
            this.radioButton_send_byte.Name = "radioButton_send_byte";
            this.radioButton_send_byte.Size = new System.Drawing.Size(97, 17);
            this.radioButton_send_byte.TabIndex = 13;
            this.radioButton_send_byte.TabStop = true;
            this.radioButton_send_byte.Text = "send byte array";
            this.radioButton_send_byte.UseVisualStyleBackColor = true;
            // 
            // textBox_send_multiplikator
            // 
            this.textBox_send_multiplikator.Location = new System.Drawing.Point(55, 207);
            this.textBox_send_multiplikator.Name = "textBox_send_multiplikator";
            this.textBox_send_multiplikator.Size = new System.Drawing.Size(71, 20);
            this.textBox_send_multiplikator.TabIndex = 21;
            this.textBox_send_multiplikator.Text = "1";
            // 
            // groupBox_status
            // 
            this.groupBox_status.Controls.Add(this.richTextBox_fstack);
            this.groupBox_status.Controls.Add(this.richTextBox_fstackLog);
            this.groupBox_status.Location = new System.Drawing.Point(331, 12);
            this.groupBox_status.Name = "groupBox_status";
            this.groupBox_status.Size = new System.Drawing.Size(783, 432);
            this.groupBox_status.TabIndex = 24;
            this.groupBox_status.TabStop = false;
            this.groupBox_status.Text = "status";
            // 
            // richTextBox_fstack
            // 
            this.richTextBox_fstack.Location = new System.Drawing.Point(9, 316);
            this.richTextBox_fstack.Name = "richTextBox_fstack";
            this.richTextBox_fstack.Size = new System.Drawing.Size(768, 100);
            this.richTextBox_fstack.TabIndex = 23;
            this.richTextBox_fstack.Text = "";
            // 
            // richTextBox_fstackLog
            // 
            this.richTextBox_fstackLog.Location = new System.Drawing.Point(9, 19);
            this.richTextBox_fstackLog.Name = "richTextBox_fstackLog";
            this.richTextBox_fstackLog.Size = new System.Drawing.Size(768, 291);
            this.richTextBox_fstackLog.TabIndex = 22;
            this.richTextBox_fstackLog.Text = "";
            // 
            // menuStrip_main
            // 
            this.menuStrip_main.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.resetToolStripMenuItem,
            this.logFileToolStripMenuItem});
            this.menuStrip_main.Location = new System.Drawing.Point(0, 0);
            this.menuStrip_main.Name = "menuStrip_main";
            this.menuStrip_main.Size = new System.Drawing.Size(1130, 24);
            this.menuStrip_main.TabIndex = 25;
            this.menuStrip_main.Text = "menuStrip1";
            // 
            // resetToolStripMenuItem
            // 
            this.resetToolStripMenuItem.Name = "resetToolStripMenuItem";
            this.resetToolStripMenuItem.Size = new System.Drawing.Size(44, 20);
            this.resetToolStripMenuItem.Text = "reset";
            this.resetToolStripMenuItem.Click += new System.EventHandler(this.resetToolStripMenuItem_Click);
            // 
            // logFileToolStripMenuItem
            // 
            this.logFileToolStripMenuItem.Name = "logFileToolStripMenuItem";
            this.logFileToolStripMenuItem.Size = new System.Drawing.Size(55, 20);
            this.logFileToolStripMenuItem.Text = "log file";
            this.logFileToolStripMenuItem.Click += new System.EventHandler(this.logFileToolStripMenuItem_Click);
            // 
            // FrmMain
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1130, 592);
            this.Controls.Add(this.groupBox_status);
            this.Controls.Add(this.groupBox_send_msg);
            this.Controls.Add(this.groupBox_listener);
            this.Controls.Add(this.menuStrip_main);
            this.MainMenuStrip = this.menuStrip_main;
            this.Name = "FrmMain";
            this.Text = "plc interface";
            this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.FrmMain_FormClosing);
            this.groupBox_client.ResumeLayout(false);
            this.groupBox_client.PerformLayout();
            this.groupBox_listener.ResumeLayout(false);
            this.groupBox_listener.PerformLayout();
            this.groupBox_local_adress.ResumeLayout(false);
            this.groupBox_local_adress.PerformLayout();
            this.groupBox_send_msg.ResumeLayout(false);
            this.groupBox_send_msg.PerformLayout();
            this.groupBox_status.ResumeLayout(false);
            this.menuStrip_main.ResumeLayout(false);
            this.menuStrip_main.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label_ip;
        private System.Windows.Forms.GroupBox groupBox_client;
        private System.Windows.Forms.TextBox textBox_remotePort;
        private System.Windows.Forms.Label label_port;
        private System.Windows.Forms.TextBox textBox_remote_ip;
        private System.Windows.Forms.CheckBox checkBox_start_server;
        private System.Windows.Forms.Label label_srv_port;
        private System.Windows.Forms.TextBox textBox_srv_port;
        private System.Windows.Forms.TextBox textBox_frame_msg;
        private System.Windows.Forms.TextBox textBox_send;
        private System.Windows.Forms.Label label_msg;
        private System.Windows.Forms.Button button_send_request;
        private System.Windows.Forms.GroupBox groupBox_listener;
        private System.Windows.Forms.ComboBox comboBox_frame_type;
        private System.Windows.Forms.ComboBox comboBox_local_ips;
        private System.Windows.Forms.Label label_host_name;
        private System.Windows.Forms.GroupBox groupBox_local_adress;
        private System.Windows.Forms.Label label_host_name_desc;
        private System.Windows.Forms.CheckBox checkBox_send_big_endian;
        private System.Windows.Forms.CheckBox checkBox_receive_big_endian;
        private System.Windows.Forms.TextBox textBox_send_index;
        private System.Windows.Forms.GroupBox groupBox_send_msg;
        private System.Windows.Forms.TextBox textBox_msg_payload;
        private System.Windows.Forms.TextBox textBox_msg_payload_ASCII;
        private System.Windows.Forms.Label label_payload_ascii;
        private System.Windows.Forms.Label label_payload_byte;
        private System.Windows.Forms.RadioButton radioButton_send_ascii;
        private System.Windows.Forms.RadioButton radioButton_send_byte;
        private System.Windows.Forms.TextBox textBox_send_multiplikator;
        private System.Windows.Forms.Label label_repeat;
        private System.Windows.Forms.Label label_cyclic_desc;
        private System.Windows.Forms.CheckBox checkBox_cyclic;
        private System.Windows.Forms.TextBox textBox_timer_interval;
        private System.Windows.Forms.Button button_check;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Button button_send_repeat;
        private System.Windows.Forms.Label label_send_times;
        private System.Windows.Forms.GroupBox groupBox_status;
        private System.Windows.Forms.RichTextBox richTextBox_fstackLog;
        private System.Windows.Forms.RichTextBox richTextBox_fstack;
        private System.Windows.Forms.MenuStrip menuStrip_main;
        private System.Windows.Forms.ToolStripMenuItem resetToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem logFileToolStripMenuItem;
    }
}

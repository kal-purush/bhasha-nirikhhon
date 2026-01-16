namespace WSClient
{
    partial class WSClient
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;
        private nsoftware.IPWorksWS.Wsclient wsclient1;
        private System.Windows.Forms.Label Label1;
        private System.Windows.Forms.Label lText;
        private System.Windows.Forms.Button bSend;
        private System.Windows.Forms.TextBox tbEcho;
        private System.Windows.Forms.Label lServer;
        private System.Windows.Forms.Button bConnect;
        private System.Windows.Forms.TextBox tbServer;

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
            this.wsclient1 = new nsoftware.IPWorksWS.Wsclient(this.components);
            this.Label1 = new System.Windows.Forms.Label();
            this.lText = new System.Windows.Forms.Label();
            this.bSend = new System.Windows.Forms.Button();
            this.tbEcho = new System.Windows.Forms.TextBox();
            this.lServer = new System.Windows.Forms.Label();
            this.bConnect = new System.Windows.Forms.Button();
            this.tbServer = new System.Windows.Forms.TextBox();
            this.tbText = new System.Windows.Forms.RichTextBox();
            this.SuspendLayout();
            // 
            // wsclient1
            // 
            this.wsclient1.About = "IP*Works! WS V9 [Build 5962]";
            this.wsclient1.Firewall.Port = 1080;
            this.wsclient1.OnConnected += new nsoftware.IPWorksWS.Wsclient.OnConnectedHandler(this.wsclient1_OnConnected);
            this.wsclient1.OnDataIn += new nsoftware.IPWorksWS.Wsclient.OnDataInHandler(this.wsclient1_OnDataIn);
            this.wsclient1.OnDisconnected += new nsoftware.IPWorksWS.Wsclient.OnDisconnectedHandler(this.wsclient1_OnDisconnected);
            // 
            // Label1
            // 
            this.Label1.Location = new System.Drawing.Point(9, 62);
            this.Label1.Name = "Label1";
            this.Label1.Size = new System.Drawing.Size(144, 16);
            this.Label1.TabIndex = 24;
            this.Label1.Text = "Data received from server:";
            // 
            // lText
            // 
            this.lText.Location = new System.Drawing.Point(9, 39);
            this.lText.Name = "lText";
            this.lText.Size = new System.Drawing.Size(80, 23);
            this.lText.TabIndex = 14;
            this.lText.Text = "Echo Text:";
            // 
            // bSend
            // 
            this.bSend.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.bSend.Location = new System.Drawing.Point(401, 40);
            this.bSend.Name = "bSend";
            this.bSend.Size = new System.Drawing.Size(75, 23);
            this.bSend.TabIndex = 22;
            this.bSend.Text = "Send";
            this.bSend.Click += new System.EventHandler(this.bSend_Click);
            // 
            // tbEcho
            // 
            this.tbEcho.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.tbEcho.Location = new System.Drawing.Point(12, 81);
            this.tbEcho.Multiline = true;
            this.tbEcho.Name = "tbEcho";
            this.tbEcho.ReadOnly = true;
            this.tbEcho.ScrollBars = System.Windows.Forms.ScrollBars.Horizontal;
            this.tbEcho.Size = new System.Drawing.Size(488, 200);
            this.tbEcho.TabIndex = 21;
            // 
            // lServer
            // 
            this.lServer.Location = new System.Drawing.Point(9, 16);
            this.lServer.Name = "lServer";
            this.lServer.Size = new System.Drawing.Size(80, 23);
            this.lServer.TabIndex = 15;
            this.lServer.Text = "Echo Server:";
            // 
            // bConnect
            // 
            this.bConnect.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.bConnect.Location = new System.Drawing.Point(401, 11);
            this.bConnect.Name = "bConnect";
            this.bConnect.Size = new System.Drawing.Size(75, 23);
            this.bConnect.TabIndex = 23;
            this.bConnect.Tag = "disconnected";
            this.bConnect.Text = "Connect";
            this.bConnect.Click += new System.EventHandler(this.bConnect_Click);
            // 
            // tbServer
            // 
            this.tbServer.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.tbServer.Location = new System.Drawing.Point(96, 14);
            this.tbServer.Name = "tbServer";
            this.tbServer.Size = new System.Drawing.Size(192, 20);
            this.tbServer.TabIndex = 19;
            this.tbServer.Text = "ws://localhost:4444";
            // 
            // tbText
            // 
            this.tbText.Location = new System.Drawing.Point(95, 36);
            this.tbText.Name = "tbText";
            this.tbText.Size = new System.Drawing.Size(193, 23);
            this.tbText.TabIndex = 25;
            this.tbText.Text = "";
            // 
            // WSClient
            // 
            this.AutoScaleBaseSize = new System.Drawing.Size(5, 13);
            this.ClientSize = new System.Drawing.Size(504, 293);
            this.Controls.Add(this.tbText);
            this.Controls.Add(this.Label1);
            this.Controls.Add(this.lText);
            this.Controls.Add(this.bSend);
            this.Controls.Add(this.tbEcho);
            this.Controls.Add(this.lServer);
            this.Controls.Add(this.bConnect);
            this.Controls.Add(this.tbServer);
            this.Name = "WSClient";
            this.Text = "wsclient1";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.RichTextBox tbText;
    }
}

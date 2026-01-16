namespace Ezzen
{
    partial class MainWindow
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(MainWindow));
            this.TitleBar = new System.Windows.Forms.Panel();
            this.Logo = new System.Windows.Forms.Panel();
            this.Title = new System.Windows.Forms.Label();
            this.MinimizeButton = new System.Windows.Forms.Button();
            this.CloseButton = new System.Windows.Forms.Button();
            this.MenuBar = new System.Windows.Forms.Panel();
            this.SettingButton = new System.Windows.Forms.Button();
            this.ChatButton = new System.Windows.Forms.Button();
            this.ExpandButton = new System.Windows.Forms.Button();
            this.ChatListPanel = new System.Windows.Forms.Panel();
            this.SearchIcon = new System.Windows.Forms.Panel();
            this.SearchBox = new System.Windows.Forms.Panel();
            this.SearchPanel = new System.Windows.Forms.TextBox();
            this.DropDown = new System.Windows.Forms.Panel();
            this.MessagesBox = new System.Windows.Forms.Panel();
            this.ChatBox = new System.Windows.Forms.RichTextBox();
            this.MessagePanel = new System.Windows.Forms.Panel();
            this.MsgPanel = new System.Windows.Forms.TextBox();
            this.SendButton = new System.Windows.Forms.Button();
            this.TitleBar.SuspendLayout();
            this.MenuBar.SuspendLayout();
            this.SearchBox.SuspendLayout();
            this.MessagesBox.SuspendLayout();
            this.MessagePanel.SuspendLayout();
            this.SuspendLayout();
            // 
            // TitleBar
            // 
            this.TitleBar.BackColor = System.Drawing.SystemColors.ButtonHighlight;
            this.TitleBar.Controls.Add(this.Logo);
            this.TitleBar.Controls.Add(this.Title);
            this.TitleBar.Controls.Add(this.MinimizeButton);
            this.TitleBar.Controls.Add(this.CloseButton);
            this.TitleBar.Location = new System.Drawing.Point(0, 0);
            this.TitleBar.Name = "TitleBar";
            this.TitleBar.Size = new System.Drawing.Size(1280, 30);
            this.TitleBar.TabIndex = 0;
            this.TitleBar.MouseDown += new System.Windows.Forms.MouseEventHandler(this.TitleBar_MouseDown);
            this.TitleBar.MouseMove += new System.Windows.Forms.MouseEventHandler(this.TitleBar_MouseMove);
            this.TitleBar.MouseUp += new System.Windows.Forms.MouseEventHandler(this.TitleBar_MouseUp);
            // 
            // Logo
            // 
            this.Logo.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("Logo.BackgroundImage")));
            this.Logo.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Zoom;
            this.Logo.Location = new System.Drawing.Point(5, 0);
            this.Logo.Name = "Logo";
            this.Logo.Size = new System.Drawing.Size(30, 30);
            this.Logo.TabIndex = 1;
            // 
            // Title
            // 
            this.Title.AutoSize = true;
            this.Title.Font = new System.Drawing.Font("Rockwell", 10.2F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Title.Location = new System.Drawing.Point(40, 5);
            this.Title.Name = "Title";
            this.Title.Size = new System.Drawing.Size(56, 20);
            this.Title.TabIndex = 2;
            this.Title.Text = "Ezzen";
            // 
            // MinimizeButton
            // 
            this.MinimizeButton.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.MinimizeButton.FlatAppearance.BorderSize = 0;
            this.MinimizeButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.MinimizeButton.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.MinimizeButton.ForeColor = System.Drawing.SystemColors.ButtonShadow;
            this.MinimizeButton.Location = new System.Drawing.Point(1220, 0);
            this.MinimizeButton.Name = "MinimizeButton";
            this.MinimizeButton.Size = new System.Drawing.Size(30, 30);
            this.MinimizeButton.TabIndex = 1;
            this.MinimizeButton.Text = "-";
            this.MinimizeButton.UseVisualStyleBackColor = true;
            this.MinimizeButton.Click += new System.EventHandler(this.MinimizeButton_Click);
            // 
            // CloseButton
            // 
            this.CloseButton.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Zoom;
            this.CloseButton.FlatAppearance.BorderSize = 0;
            this.CloseButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.CloseButton.Font = new System.Drawing.Font("Microsoft Sans Serif", 7.8F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.CloseButton.ForeColor = System.Drawing.SystemColors.ButtonShadow;
            this.CloseButton.Location = new System.Drawing.Point(1250, 0);
            this.CloseButton.Name = "CloseButton";
            this.CloseButton.Size = new System.Drawing.Size(30, 30);
            this.CloseButton.TabIndex = 0;
            this.CloseButton.Text = "X";
            this.CloseButton.UseVisualStyleBackColor = true;
            this.CloseButton.Click += new System.EventHandler(this.CloseButton_Click);
            // 
            // MenuBar
            // 
            this.MenuBar.BackColor = System.Drawing.SystemColors.ActiveBorder;
            this.MenuBar.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("MenuBar.BackgroundImage")));
            this.MenuBar.Controls.Add(this.SettingButton);
            this.MenuBar.Controls.Add(this.ChatButton);
            this.MenuBar.Controls.Add(this.ExpandButton);
            this.MenuBar.Location = new System.Drawing.Point(0, 30);
            this.MenuBar.Name = "MenuBar";
            this.MenuBar.Size = new System.Drawing.Size(50, 738);
            this.MenuBar.TabIndex = 1;
            // 
            // SettingButton
            // 
            this.SettingButton.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("SettingButton.BackgroundImage")));
            this.SettingButton.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.SettingButton.FlatAppearance.BorderSize = 0;
            this.SettingButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.SettingButton.Location = new System.Drawing.Point(5, 369);
            this.SettingButton.Name = "SettingButton";
            this.SettingButton.Size = new System.Drawing.Size(40, 40);
            this.SettingButton.TabIndex = 2;
            this.SettingButton.UseVisualStyleBackColor = true;
            // 
            // ChatButton
            // 
            this.ChatButton.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("ChatButton.BackgroundImage")));
            this.ChatButton.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.ChatButton.FlatAppearance.BorderSize = 0;
            this.ChatButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.ChatButton.Location = new System.Drawing.Point(5, 329);
            this.ChatButton.Name = "ChatButton";
            this.ChatButton.Size = new System.Drawing.Size(40, 40);
            this.ChatButton.TabIndex = 2;
            this.ChatButton.UseVisualStyleBackColor = true;
            // 
            // ExpandButton
            // 
            this.ExpandButton.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("ExpandButton.BackgroundImage")));
            this.ExpandButton.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.ExpandButton.FlatAppearance.BorderSize = 0;
            this.ExpandButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.ExpandButton.Location = new System.Drawing.Point(5, 0);
            this.ExpandButton.Name = "ExpandButton";
            this.ExpandButton.Size = new System.Drawing.Size(40, 40);
            this.ExpandButton.TabIndex = 2;
            this.ExpandButton.UseVisualStyleBackColor = true;
            // 
            // ChatListPanel
            // 
            this.ChatListPanel.Location = new System.Drawing.Point(50, 68);
            this.ChatListPanel.Name = "ChatListPanel";
            this.ChatListPanel.Size = new System.Drawing.Size(400, 700);
            this.ChatListPanel.TabIndex = 2;
            // 
            // SearchIcon
            // 
            this.SearchIcon.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("SearchIcon.BackgroundImage")));
            this.SearchIcon.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.SearchIcon.Location = new System.Drawing.Point(50, 30);
            this.SearchIcon.Name = "SearchIcon";
            this.SearchIcon.Size = new System.Drawing.Size(38, 38);
            this.SearchIcon.TabIndex = 3;
            // 
            // SearchBox
            // 
            this.SearchBox.BackColor = System.Drawing.Color.White;
            this.SearchBox.Controls.Add(this.SearchPanel);
            this.SearchBox.Location = new System.Drawing.Point(88, 30);
            this.SearchBox.Name = "SearchBox";
            this.SearchBox.Size = new System.Drawing.Size(328, 38);
            this.SearchBox.TabIndex = 4;
            // 
            // SearchPanel
            // 
            this.SearchPanel.BackColor = System.Drawing.SystemColors.Window;
            this.SearchPanel.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.SearchPanel.Font = new System.Drawing.Font("Goudy Old Style", 28F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Pixel, ((byte)(0)));
            this.SearchPanel.ForeColor = System.Drawing.SystemColors.WindowText;
            this.SearchPanel.Location = new System.Drawing.Point(9, 2);
            this.SearchPanel.Margin = new System.Windows.Forms.Padding(10);
            this.SearchPanel.Name = "SearchPanel";
            this.SearchPanel.Size = new System.Drawing.Size(310, 34);
            this.SearchPanel.TabIndex = 0;
            this.SearchPanel.Text = "\r\n";
            // 
            // DropDown
            // 
            this.DropDown.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("DropDown.BackgroundImage")));
            this.DropDown.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.DropDown.Location = new System.Drawing.Point(412, 30);
            this.DropDown.Name = "DropDown";
            this.DropDown.Size = new System.Drawing.Size(38, 38);
            this.DropDown.TabIndex = 4;
            // 
            // MessagesBox
            // 
            this.MessagesBox.BackColor = System.Drawing.SystemColors.Control;
            this.MessagesBox.Controls.Add(this.ChatBox);
            this.MessagesBox.Location = new System.Drawing.Point(450, 30);
            this.MessagesBox.Name = "MessagesBox";
            this.MessagesBox.Size = new System.Drawing.Size(830, 700);
            this.MessagesBox.TabIndex = 5;
            // 
            // ChatBox
            // 
            this.ChatBox.BackColor = System.Drawing.SystemColors.ButtonHighlight;
            this.ChatBox.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.ChatBox.Font = new System.Drawing.Font("Microsoft Sans Serif", 16F);
            this.ChatBox.Location = new System.Drawing.Point(5, 5);
            this.ChatBox.Name = "ChatBox";
            this.ChatBox.ReadOnly = true;
            this.ChatBox.Size = new System.Drawing.Size(820, 690);
            this.ChatBox.TabIndex = 0;
            this.ChatBox.Text = "";
            // 
            // MessagePanel
            // 
            this.MessagePanel.BackColor = System.Drawing.Color.White;
            this.MessagePanel.Controls.Add(this.MsgPanel);
            this.MessagePanel.Location = new System.Drawing.Point(450, 730);
            this.MessagePanel.Name = "MessagePanel";
            this.MessagePanel.Size = new System.Drawing.Size(750, 38);
            this.MessagePanel.TabIndex = 6;
            // 
            // MsgPanel
            // 
            this.MsgPanel.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.MsgPanel.Font = new System.Drawing.Font("Goudy Old Style", 18F);
            this.MsgPanel.ForeColor = System.Drawing.SystemColors.WindowText;
            this.MsgPanel.Location = new System.Drawing.Point(10, 2);
            this.MsgPanel.Margin = new System.Windows.Forms.Padding(10);
            this.MsgPanel.Multiline = false;
            this.MsgPanel.Name = "MsgPanel";
            this.MsgPanel.Size = new System.Drawing.Size(730, 34);
            this.MsgPanel.TabIndex = 0;
            this.MsgPanel.KeyDown += new System.Windows.Forms.KeyEventHandler(this.MsgPanel_KeyDown);
            this.MsgPanel.KeyPress += new System.Windows.Forms.KeyPressEventHandler(this.MsgPanel_KeyPress);
            this.MsgPanel.KeyUp += new System.Windows.Forms.KeyEventHandler(this.MsgPanel_KeyUp);
            // 
            // SendButton
            // 
            this.SendButton.BackColor = System.Drawing.Color.LimeGreen;
            this.SendButton.FlatAppearance.BorderSize = 0;
            this.SendButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.SendButton.Font = new System.Drawing.Font("Tw Cen MT Condensed Extra Bold", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.SendButton.ForeColor = System.Drawing.SystemColors.ButtonHighlight;
            this.SendButton.Location = new System.Drawing.Point(1200, 730);
            this.SendButton.Name = "SendButton";
            this.SendButton.Size = new System.Drawing.Size(80, 38);
            this.SendButton.TabIndex = 7;
            this.SendButton.Text = "Send";
            this.SendButton.UseVisualStyleBackColor = false;
            this.SendButton.Click += new System.EventHandler(this.SendButton_Click);
            // 
            // MainWindow
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1280, 768);
            this.Controls.Add(this.SendButton);
            this.Controls.Add(this.MessagePanel);
            this.Controls.Add(this.MessagesBox);
            this.Controls.Add(this.DropDown);
            this.Controls.Add(this.SearchBox);
            this.Controls.Add(this.SearchIcon);
            this.Controls.Add(this.ChatListPanel);
            this.Controls.Add(this.MenuBar);
            this.Controls.Add(this.TitleBar);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.None;
            this.Name = "MainWindow";
            this.TitleBar.ResumeLayout(false);
            this.TitleBar.PerformLayout();
            this.MenuBar.ResumeLayout(false);
            this.SearchBox.ResumeLayout(false);
            this.SearchBox.PerformLayout();
            this.MessagesBox.ResumeLayout(false);
            this.MessagePanel.ResumeLayout(false);
            this.MessagePanel.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Panel TitleBar;
        private System.Windows.Forms.Button CloseButton;
        private System.Windows.Forms.Button MinimizeButton;
        private System.Windows.Forms.Label Title;
        private System.Windows.Forms.Panel Logo;
        private System.Windows.Forms.Panel MenuBar;
        private System.Windows.Forms.Button ExpandButton;
        private System.Windows.Forms.Button SettingButton;
        private System.Windows.Forms.Button ChatButton;
        private System.Windows.Forms.Panel ChatListPanel;
        private System.Windows.Forms.Panel SearchIcon;
        private System.Windows.Forms.Panel SearchBox;
        private System.Windows.Forms.Panel DropDown;
        private System.Windows.Forms.Panel MessagesBox;
        private System.Windows.Forms.Panel MessagePanel;
        private System.Windows.Forms.Button SendButton;
        private System.Windows.Forms.TextBox SearchPanel;
        private System.Windows.Forms.TextBox MsgPanel;
        private System.Windows.Forms.RichTextBox ChatBox;
    }
}

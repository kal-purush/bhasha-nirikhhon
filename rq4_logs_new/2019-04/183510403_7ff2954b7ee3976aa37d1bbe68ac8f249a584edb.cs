namespace Trivago.Forms
{
    partial class ReserveRoom
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(ReserveRoom));
            Bunifu.UI.WinForms.BunifuButton.BunifuButton.StateProperties stateProperties1 = new Bunifu.UI.WinForms.BunifuButton.BunifuButton.StateProperties();
            this.SearchTextbox = new Bunifu.Framework.UI.BunifuMaterialTextbox();
            this.bunifuCustomLabel1 = new Bunifu.Framework.UI.BunifuCustomLabel();
            this.DisplayPanel = new Bunifu.UI.WinForm.BunifuShadowPanel.BunifuShadowPanel();
            this.RoomTypeLabel = new Bunifu.Framework.UI.BunifuCustomLabel();
            this.RoomTypeDropdown = new Bunifu.UI.WinForms.BunifuDropdown();
            this.tempPanel = new Bunifu.UI.WinForm.BunifuShadowPanel.BunifuShadowPanel();
            this.HotelName = new Bunifu.Framework.UI.BunifuCustomLabel();
            this.HotelRating = new Bunifu.Framework.UI.BunifuRating();
            this.viewOffersButton = new Bunifu.UI.WinForms.BunifuButton.BunifuButton();
            this.RoomId = new Bunifu.Framework.UI.BunifuCustomLabel();
            this.RoomType = new Bunifu.Framework.UI.BunifuCustomLabel();
            this.HotelLocation = new Bunifu.Framework.UI.BunifuCustomLabel();
            this.RoomPrice = new Bunifu.Framework.UI.BunifuCustomLabel();
            this.WebsiteName = new Bunifu.Framework.UI.BunifuCustomLabel();
            this.DisplayPanel.SuspendLayout();
            this.tempPanel.SuspendLayout();
            this.SuspendLayout();
            // 
            // SearchTextbox
            // 
            this.SearchTextbox.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.None;
            this.SearchTextbox.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.None;
            this.SearchTextbox.characterCasing = System.Windows.Forms.CharacterCasing.Normal;
            this.SearchTextbox.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.SearchTextbox.Font = new System.Drawing.Font("Century Gothic", 9.75F);
            this.SearchTextbox.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.SearchTextbox.HintForeColor = System.Drawing.Color.Empty;
            this.SearchTextbox.HintText = "Enter Hotel name or country or city or street";
            this.SearchTextbox.isPassword = false;
            this.SearchTextbox.LineFocusedColor = System.Drawing.Color.Blue;
            this.SearchTextbox.LineIdleColor = System.Drawing.Color.Gray;
            this.SearchTextbox.LineMouseHoverColor = System.Drawing.Color.Blue;
            this.SearchTextbox.LineThickness = 3;
            this.SearchTextbox.Location = new System.Drawing.Point(13, 30);
            this.SearchTextbox.Margin = new System.Windows.Forms.Padding(4);
            this.SearchTextbox.MaxLength = 32767;
            this.SearchTextbox.Name = "SearchTextbox";
            this.SearchTextbox.Size = new System.Drawing.Size(362, 33);
            this.SearchTextbox.TabIndex = 0;
            this.SearchTextbox.TextAlign = System.Windows.Forms.HorizontalAlignment.Left;
            this.SearchTextbox.OnValueChanged += new System.EventHandler(this.SearchTextbox_OnValueChanged);
            // 
            // bunifuCustomLabel1
            // 
            this.bunifuCustomLabel1.AutoSize = true;
            this.bunifuCustomLabel1.Location = new System.Drawing.Point(12, 9);
            this.bunifuCustomLabel1.Name = "bunifuCustomLabel1";
            this.bunifuCustomLabel1.Size = new System.Drawing.Size(61, 17);
            this.bunifuCustomLabel1.TabIndex = 1;
            this.bunifuCustomLabel1.Text = "Search :";
            // 
            // DisplayPanel
            // 
            this.DisplayPanel.AutoScroll = true;
            this.DisplayPanel.BorderColor = System.Drawing.Color.Gainsboro;
            this.DisplayPanel.Controls.Add(this.tempPanel);
            this.DisplayPanel.Location = new System.Drawing.Point(15, 225);
            this.DisplayPanel.Name = "DisplayPanel";
            this.DisplayPanel.PanelColor = System.Drawing.Color.Empty;
            this.DisplayPanel.ShadowDept = 2;
            this.DisplayPanel.ShadowTopLeftVisible = false;
            this.DisplayPanel.Size = new System.Drawing.Size(1080, 367);
            this.DisplayPanel.TabIndex = 2;
            // 
            // RoomTypeLabel
            // 
            this.RoomTypeLabel.AutoSize = true;
            this.RoomTypeLabel.Location = new System.Drawing.Point(12, 77);
            this.RoomTypeLabel.Name = "RoomTypeLabel";
            this.RoomTypeLabel.Size = new System.Drawing.Size(89, 17);
            this.RoomTypeLabel.TabIndex = 4;
            this.RoomTypeLabel.Text = "Room Type :";
            // 
            // RoomTypeDropdown
            // 
            this.RoomTypeDropdown.BackColor = System.Drawing.SystemColors.Control;
            this.RoomTypeDropdown.BorderRadius = 1;
            this.RoomTypeDropdown.Direction = Bunifu.UI.WinForms.BunifuDropdown.Directions.Down;
            this.RoomTypeDropdown.DisabledColor = System.Drawing.Color.Gray;
            this.RoomTypeDropdown.DrawMode = System.Windows.Forms.DrawMode.OwnerDrawFixed;
            this.RoomTypeDropdown.DropdownBorderThickness = Bunifu.UI.WinForms.BunifuDropdown.BorderThickness.Thick;
            this.RoomTypeDropdown.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.RoomTypeDropdown.DropDownTextAlign = Bunifu.UI.WinForms.BunifuDropdown.TextAlign.Left;
            this.RoomTypeDropdown.FillDropDown = false;
            this.RoomTypeDropdown.FillIndicator = false;
            this.RoomTypeDropdown.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.RoomTypeDropdown.ForeColor = System.Drawing.Color.Purple;
            this.RoomTypeDropdown.FormattingEnabled = true;
            this.RoomTypeDropdown.Icon = null;
            this.RoomTypeDropdown.IndicatorColor = System.Drawing.Color.Purple;
            this.RoomTypeDropdown.IndicatorLocation = Bunifu.UI.WinForms.BunifuDropdown.Indicator.Right;
            this.RoomTypeDropdown.ItemBackColor = System.Drawing.Color.White;
            this.RoomTypeDropdown.ItemBorderColor = System.Drawing.Color.White;
            this.RoomTypeDropdown.ItemForeColor = System.Drawing.Color.Purple;
            this.RoomTypeDropdown.ItemHeight = 26;
            this.RoomTypeDropdown.ItemHighLightColor = System.Drawing.Color.Thistle;
            this.RoomTypeDropdown.Location = new System.Drawing.Point(15, 98);
            this.RoomTypeDropdown.Name = "RoomTypeDropdown";
            this.RoomTypeDropdown.Size = new System.Drawing.Size(217, 32);
            this.RoomTypeDropdown.TabIndex = 5;
            // 
            // tempPanel
            // 
            this.tempPanel.BorderColor = System.Drawing.Color.Gainsboro;
            this.tempPanel.Controls.Add(this.WebsiteName);
            this.tempPanel.Controls.Add(this.RoomPrice);
            this.tempPanel.Controls.Add(this.HotelLocation);
            this.tempPanel.Controls.Add(this.RoomType);
            this.tempPanel.Controls.Add(this.RoomId);
            this.tempPanel.Controls.Add(this.HotelRating);
            this.tempPanel.Controls.Add(this.HotelName);
            this.tempPanel.Dock = System.Windows.Forms.DockStyle.Top;
            this.tempPanel.Location = new System.Drawing.Point(0, 0);
            this.tempPanel.Name = "tempPanel";
            this.tempPanel.PanelColor = System.Drawing.Color.Empty;
            this.tempPanel.ShadowDept = 2;
            this.tempPanel.ShadowTopLeftVisible = false;
            this.tempPanel.Size = new System.Drawing.Size(1080, 135);
            this.tempPanel.TabIndex = 0;
            // 
            // HotelName
            // 
            this.HotelName.AutoSize = true;
            this.HotelName.Location = new System.Drawing.Point(4, 4);
            this.HotelName.Name = "HotelName";
            this.HotelName.Size = new System.Drawing.Size(78, 17);
            this.HotelName.TabIndex = 0;
            this.HotelName.Text = "HotelName";
            // 
            // HotelRating
            // 
            this.HotelRating.BackColor = System.Drawing.Color.Transparent;
            this.HotelRating.ForeColor = System.Drawing.Color.SeaGreen;
            this.HotelRating.Location = new System.Drawing.Point(4, 35);
            this.HotelRating.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            this.HotelRating.Name = "HotelRating";
            this.HotelRating.Size = new System.Drawing.Size(244, 37);
            this.HotelRating.TabIndex = 1;
            this.HotelRating.Value = 0;
            // 
            // viewOffersButton
            // 
            this.viewOffersButton.BackColor = System.Drawing.Color.Transparent;
            this.viewOffersButton.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("viewOffersButton.BackgroundImage")));
            this.viewOffersButton.ButtonText = "View Offers";
            this.viewOffersButton.ButtonTextMarginLeft = 0;
            this.viewOffersButton.DisabledBorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(161)))), ((int)(((byte)(161)))), ((int)(((byte)(161)))));
            this.viewOffersButton.DisabledFillColor = System.Drawing.Color.Gray;
            this.viewOffersButton.DisabledForecolor = System.Drawing.Color.White;
            this.viewOffersButton.ForeColor = System.Drawing.Color.White;
            this.viewOffersButton.IconLeftCursor = System.Windows.Forms.Cursors.Default;
            this.viewOffersButton.IconPadding = 10;
            this.viewOffersButton.IconRightCursor = System.Windows.Forms.Cursors.Default;
            this.viewOffersButton.IdleBorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(51)))), ((int)(((byte)(122)))), ((int)(((byte)(183)))));
            this.viewOffersButton.IdleBorderRadius = 1;
            this.viewOffersButton.IdleBorderThickness = 0;
            this.viewOffersButton.IdleFillColor = System.Drawing.Color.FromArgb(((int)(((byte)(51)))), ((int)(((byte)(122)))), ((int)(((byte)(183)))));
            this.viewOffersButton.IdleIconLeftImage = null;
            this.viewOffersButton.IdleIconRightImage = null;
            this.viewOffersButton.Location = new System.Drawing.Point(22, 154);
            this.viewOffersButton.Name = "viewOffersButton";
            stateProperties1.BorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(40)))), ((int)(((byte)(96)))), ((int)(((byte)(144)))));
            stateProperties1.BorderRadius = 1;
            stateProperties1.BorderThickness = 1;
            stateProperties1.FillColor = System.Drawing.Color.FromArgb(((int)(((byte)(40)))), ((int)(((byte)(96)))), ((int)(((byte)(144)))));
            stateProperties1.IconLeftImage = null;
            stateProperties1.IconRightImage = null;
            this.viewOffersButton.onHoverState = stateProperties1;
            this.viewOffersButton.Size = new System.Drawing.Size(210, 45);
            this.viewOffersButton.TabIndex = 6;
            this.viewOffersButton.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.viewOffersButton.Click += new System.EventHandler(this.viewOffersButton_Click);
            // 
            // RoomId
            // 
            this.RoomId.AutoSize = true;
            this.RoomId.Location = new System.Drawing.Point(300, 4);
            this.RoomId.Name = "RoomId";
            this.RoomId.Size = new System.Drawing.Size(62, 17);
            this.RoomId.TabIndex = 2;
            this.RoomId.Text = "Room ID";
            // 
            // RoomType
            // 
            this.RoomType.AutoSize = true;
            this.RoomType.Location = new System.Drawing.Point(300, 46);
            this.RoomType.Name = "RoomType";
            this.RoomType.Size = new System.Drawing.Size(81, 17);
            this.RoomType.TabIndex = 3;
            this.RoomType.Text = "Room Type";
            // 
            // HotelLocation
            // 
            this.HotelLocation.AutoSize = true;
            this.HotelLocation.Location = new System.Drawing.Point(3, 86);
            this.HotelLocation.Name = "HotelLocation";
            this.HotelLocation.Size = new System.Drawing.Size(95, 17);
            this.HotelLocation.TabIndex = 4;
            this.HotelLocation.Text = "HotelLocation";
            // 
            // RoomPrice
            // 
            this.RoomPrice.AutoSize = true;
            this.RoomPrice.Location = new System.Drawing.Point(300, 86);
            this.RoomPrice.Name = "RoomPrice";
            this.RoomPrice.Size = new System.Drawing.Size(77, 17);
            this.RoomPrice.TabIndex = 5;
            this.RoomPrice.Text = "RoomPrice";
            // 
            // WebsiteName
            // 
            this.WebsiteName.AutoSize = true;
            this.WebsiteName.Location = new System.Drawing.Point(530, 45);
            this.WebsiteName.Name = "WebsiteName";
            this.WebsiteName.Size = new System.Drawing.Size(96, 17);
            this.WebsiteName.TabIndex = 6;
            this.WebsiteName.Text = "WebsiteName";
            // 
            // ReserveRoom
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1107, 604);
            this.Controls.Add(this.viewOffersButton);
            this.Controls.Add(this.RoomTypeDropdown);
            this.Controls.Add(this.RoomTypeLabel);
            this.Controls.Add(this.DisplayPanel);
            this.Controls.Add(this.bunifuCustomLabel1);
            this.Controls.Add(this.SearchTextbox);
            this.Name = "ReserveRoom";
            this.Text = "ReserveRoom";
            this.DisplayPanel.ResumeLayout(false);
            this.tempPanel.ResumeLayout(false);
            this.tempPanel.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private Bunifu.Framework.UI.BunifuMaterialTextbox SearchTextbox;
        private Bunifu.Framework.UI.BunifuCustomLabel bunifuCustomLabel1;
        private Bunifu.UI.WinForm.BunifuShadowPanel.BunifuShadowPanel DisplayPanel;
        private Bunifu.Framework.UI.BunifuCustomLabel RoomTypeLabel;
        private Bunifu.UI.WinForms.BunifuDropdown RoomTypeDropdown;
        private Bunifu.UI.WinForm.BunifuShadowPanel.BunifuShadowPanel tempPanel;
        private Bunifu.Framework.UI.BunifuCustomLabel HotelName;
        private Bunifu.Framework.UI.BunifuRating HotelRating;
        private Bunifu.UI.WinForms.BunifuButton.BunifuButton viewOffersButton;
        private Bunifu.Framework.UI.BunifuCustomLabel RoomPrice;
        private Bunifu.Framework.UI.BunifuCustomLabel HotelLocation;
        private Bunifu.Framework.UI.BunifuCustomLabel RoomType;
        private Bunifu.Framework.UI.BunifuCustomLabel RoomId;
        private Bunifu.Framework.UI.BunifuCustomLabel WebsiteName;
    }
}
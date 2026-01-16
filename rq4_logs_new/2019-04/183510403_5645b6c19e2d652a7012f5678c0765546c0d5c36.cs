namespace Trivago.Forms
{
    partial class EditRoom
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(EditRoom));
            Bunifu.UI.WinForms.BunifuButton.BunifuButton.StateProperties stateProperties2 = new Bunifu.UI.WinForms.BunifuButton.BunifuButton.StateProperties();
            this.HotelNameDropDown = new Bunifu.UI.WinForms.BunifuDropdown();
            this.RoomIdDropDown = new Bunifu.UI.WinForms.BunifuDropdown();
            this.RoomTypeDropDown = new Bunifu.UI.WinForms.BunifuDropdown();
            this.SubmitButton = new Bunifu.UI.WinForms.BunifuButton.BunifuButton();
            this.SuspendLayout();
            // 
            // HotelNameDropDown
            // 
            this.HotelNameDropDown.BackColor = System.Drawing.SystemColors.Control;
            this.HotelNameDropDown.BorderRadius = 1;
            this.HotelNameDropDown.Direction = Bunifu.UI.WinForms.BunifuDropdown.Directions.Down;
            this.HotelNameDropDown.DisabledColor = System.Drawing.Color.Gray;
            this.HotelNameDropDown.DrawMode = System.Windows.Forms.DrawMode.OwnerDrawFixed;
            this.HotelNameDropDown.DropdownBorderThickness = Bunifu.UI.WinForms.BunifuDropdown.BorderThickness.Thick;
            this.HotelNameDropDown.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.HotelNameDropDown.DropDownTextAlign = Bunifu.UI.WinForms.BunifuDropdown.TextAlign.Left;
            this.HotelNameDropDown.FillDropDown = false;
            this.HotelNameDropDown.FillIndicator = false;
            this.HotelNameDropDown.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.HotelNameDropDown.ForeColor = System.Drawing.Color.Purple;
            this.HotelNameDropDown.FormattingEnabled = true;
            this.HotelNameDropDown.Icon = null;
            this.HotelNameDropDown.IndicatorColor = System.Drawing.Color.Purple;
            this.HotelNameDropDown.IndicatorLocation = Bunifu.UI.WinForms.BunifuDropdown.Indicator.Right;
            this.HotelNameDropDown.ItemBackColor = System.Drawing.Color.White;
            this.HotelNameDropDown.ItemBorderColor = System.Drawing.Color.White;
            this.HotelNameDropDown.ItemForeColor = System.Drawing.Color.Purple;
            this.HotelNameDropDown.ItemHeight = 26;
            this.HotelNameDropDown.ItemHighLightColor = System.Drawing.Color.Thistle;
            this.HotelNameDropDown.Location = new System.Drawing.Point(245, 12);
            this.HotelNameDropDown.Name = "HotelNameDropDown";
            this.HotelNameDropDown.Size = new System.Drawing.Size(217, 32);
            this.HotelNameDropDown.TabIndex = 0;
            this.HotelNameDropDown.Text = "Hotel Name";
            this.HotelNameDropDown.SelectedIndexChanged += new System.EventHandler(this.HotelNameDropDown_SelectedIndexChanged);
            // 
            // RoomIdDropDown
            // 
            this.RoomIdDropDown.BackColor = System.Drawing.SystemColors.Control;
            this.RoomIdDropDown.BorderRadius = 1;
            this.RoomIdDropDown.Direction = Bunifu.UI.WinForms.BunifuDropdown.Directions.Down;
            this.RoomIdDropDown.DisabledColor = System.Drawing.Color.Gray;
            this.RoomIdDropDown.DrawMode = System.Windows.Forms.DrawMode.OwnerDrawFixed;
            this.RoomIdDropDown.DropdownBorderThickness = Bunifu.UI.WinForms.BunifuDropdown.BorderThickness.Thick;
            this.RoomIdDropDown.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.RoomIdDropDown.DropDownTextAlign = Bunifu.UI.WinForms.BunifuDropdown.TextAlign.Left;
            this.RoomIdDropDown.FillDropDown = false;
            this.RoomIdDropDown.FillIndicator = false;
            this.RoomIdDropDown.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.RoomIdDropDown.ForeColor = System.Drawing.Color.Purple;
            this.RoomIdDropDown.FormattingEnabled = true;
            this.RoomIdDropDown.Icon = null;
            this.RoomIdDropDown.IndicatorColor = System.Drawing.Color.Purple;
            this.RoomIdDropDown.IndicatorLocation = Bunifu.UI.WinForms.BunifuDropdown.Indicator.Right;
            this.RoomIdDropDown.ItemBackColor = System.Drawing.Color.White;
            this.RoomIdDropDown.ItemBorderColor = System.Drawing.Color.White;
            this.RoomIdDropDown.ItemForeColor = System.Drawing.Color.Purple;
            this.RoomIdDropDown.ItemHeight = 26;
            this.RoomIdDropDown.ItemHighLightColor = System.Drawing.Color.Thistle;
            this.RoomIdDropDown.Location = new System.Drawing.Point(245, 102);
            this.RoomIdDropDown.Name = "RoomIdDropDown";
            this.RoomIdDropDown.Size = new System.Drawing.Size(217, 32);
            this.RoomIdDropDown.TabIndex = 1;
            this.RoomIdDropDown.Text = "Room ID";
            this.RoomIdDropDown.SelectedIndexChanged += new System.EventHandler(this.RoomIdDropDown_SelectedIndexChanged);
            // 
            // RoomTypeDropDown
            // 
            this.RoomTypeDropDown.BackColor = System.Drawing.SystemColors.Control;
            this.RoomTypeDropDown.BorderRadius = 1;
            this.RoomTypeDropDown.Direction = Bunifu.UI.WinForms.BunifuDropdown.Directions.Down;
            this.RoomTypeDropDown.DisabledColor = System.Drawing.Color.Gray;
            this.RoomTypeDropDown.DrawMode = System.Windows.Forms.DrawMode.OwnerDrawFixed;
            this.RoomTypeDropDown.DropdownBorderThickness = Bunifu.UI.WinForms.BunifuDropdown.BorderThickness.Thick;
            this.RoomTypeDropDown.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.RoomTypeDropDown.DropDownTextAlign = Bunifu.UI.WinForms.BunifuDropdown.TextAlign.Left;
            this.RoomTypeDropDown.FillDropDown = false;
            this.RoomTypeDropDown.FillIndicator = false;
            this.RoomTypeDropDown.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.RoomTypeDropDown.ForeColor = System.Drawing.Color.Purple;
            this.RoomTypeDropDown.FormattingEnabled = true;
            this.RoomTypeDropDown.Icon = null;
            this.RoomTypeDropDown.IndicatorColor = System.Drawing.Color.Purple;
            this.RoomTypeDropDown.IndicatorLocation = Bunifu.UI.WinForms.BunifuDropdown.Indicator.Right;
            this.RoomTypeDropDown.ItemBackColor = System.Drawing.Color.White;
            this.RoomTypeDropDown.ItemBorderColor = System.Drawing.Color.White;
            this.RoomTypeDropDown.ItemForeColor = System.Drawing.Color.Purple;
            this.RoomTypeDropDown.ItemHeight = 26;
            this.RoomTypeDropDown.ItemHighLightColor = System.Drawing.Color.Thistle;
            this.RoomTypeDropDown.Location = new System.Drawing.Point(245, 193);
            this.RoomTypeDropDown.Name = "RoomTypeDropDown";
            this.RoomTypeDropDown.Size = new System.Drawing.Size(217, 32);
            this.RoomTypeDropDown.TabIndex = 2;
            this.RoomTypeDropDown.Text = "Room Type";
            // 
            // SubmitButton
            // 
            this.SubmitButton.BackColor = System.Drawing.Color.Transparent;
            this.SubmitButton.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("SubmitButton.BackgroundImage")));
            this.SubmitButton.ButtonText = "Submit";
            this.SubmitButton.ButtonTextMarginLeft = 0;
            this.SubmitButton.DisabledBorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(161)))), ((int)(((byte)(161)))), ((int)(((byte)(161)))));
            this.SubmitButton.DisabledFillColor = System.Drawing.Color.Gray;
            this.SubmitButton.DisabledForecolor = System.Drawing.Color.White;
            this.SubmitButton.ForeColor = System.Drawing.Color.White;
            this.SubmitButton.IconLeftCursor = System.Windows.Forms.Cursors.Default;
            this.SubmitButton.IconPadding = 10;
            this.SubmitButton.IconRightCursor = System.Windows.Forms.Cursors.Default;
            this.SubmitButton.IdleBorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(51)))), ((int)(((byte)(122)))), ((int)(((byte)(183)))));
            this.SubmitButton.IdleBorderRadius = 1;
            this.SubmitButton.IdleBorderThickness = 0;
            this.SubmitButton.IdleFillColor = System.Drawing.Color.FromArgb(((int)(((byte)(51)))), ((int)(((byte)(122)))), ((int)(((byte)(183)))));
            this.SubmitButton.IdleIconLeftImage = null;
            this.SubmitButton.IdleIconRightImage = null;
            this.SubmitButton.Location = new System.Drawing.Point(245, 292);
            this.SubmitButton.Name = "SubmitButton";
            stateProperties2.BorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(40)))), ((int)(((byte)(96)))), ((int)(((byte)(144)))));
            stateProperties2.BorderRadius = 1;
            stateProperties2.BorderThickness = 1;
            stateProperties2.FillColor = System.Drawing.Color.FromArgb(((int)(((byte)(40)))), ((int)(((byte)(96)))), ((int)(((byte)(144)))));
            stateProperties2.IconLeftImage = null;
            stateProperties2.IconRightImage = null;
            this.SubmitButton.onHoverState = stateProperties2;
            this.SubmitButton.Size = new System.Drawing.Size(217, 48);
            this.SubmitButton.TabIndex = 5;
            this.SubmitButton.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.SubmitButton.Click += new System.EventHandler(this.SubmitButton_Click);
            // 
            // EditRoom
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(693, 397);
            this.Controls.Add(this.SubmitButton);
            this.Controls.Add(this.RoomTypeDropDown);
            this.Controls.Add(this.RoomIdDropDown);
            this.Controls.Add(this.HotelNameDropDown);
            this.Name = "EditRoom";
            this.Text = "EditRoom";
            this.Load += new System.EventHandler(this.EditRoom_Load);
            this.ResumeLayout(false);

        }

        #endregion

        private Bunifu.UI.WinForms.BunifuDropdown HotelNameDropDown;
        private Bunifu.UI.WinForms.BunifuDropdown RoomIdDropDown;
        private Bunifu.UI.WinForms.BunifuDropdown RoomTypeDropDown;
        private Bunifu.UI.WinForms.BunifuButton.BunifuButton SubmitButton;
    }
}
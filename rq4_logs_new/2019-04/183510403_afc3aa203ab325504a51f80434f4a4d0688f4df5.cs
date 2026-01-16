namespace Trivago.Forms
{
    partial class DeleteRoom
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(DeleteRoom));
            Bunifu.UI.WinForms.BunifuButton.BunifuButton.StateProperties stateProperties1 = new Bunifu.UI.WinForms.BunifuButton.BunifuButton.StateProperties();
            this.hotelDropDown = new Bunifu.UI.WinForms.BunifuDropdown();
            this.roomDropDown = new Bunifu.UI.WinForms.BunifuDropdown();
            this.DeleteBTN = new Bunifu.UI.WinForms.BunifuButton.BunifuButton();
            this.SuspendLayout();
            // 
            // hotelDropDown
            // 
            this.hotelDropDown.AccessibleDescription = "";
            this.hotelDropDown.BackColor = System.Drawing.SystemColors.Control;
            this.hotelDropDown.BorderRadius = 1;
            this.hotelDropDown.Color = System.Drawing.Color.MidnightBlue;
            this.hotelDropDown.Direction = Bunifu.UI.WinForms.BunifuDropdown.Directions.Down;
            this.hotelDropDown.DisabledColor = System.Drawing.Color.Gray;
            this.hotelDropDown.DrawMode = System.Windows.Forms.DrawMode.OwnerDrawFixed;
            this.hotelDropDown.DropdownBorderThickness = Bunifu.UI.WinForms.BunifuDropdown.BorderThickness.Thick;
            this.hotelDropDown.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.hotelDropDown.DropDownTextAlign = Bunifu.UI.WinForms.BunifuDropdown.TextAlign.Left;
            this.hotelDropDown.FillDropDown = false;
            this.hotelDropDown.FillIndicator = false;
            this.hotelDropDown.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.hotelDropDown.ForeColor = System.Drawing.Color.RoyalBlue;
            this.hotelDropDown.FormattingEnabled = true;
            this.hotelDropDown.Icon = null;
            this.hotelDropDown.IndicatorColor = System.Drawing.Color.MidnightBlue;
            this.hotelDropDown.IndicatorLocation = Bunifu.UI.WinForms.BunifuDropdown.Indicator.Right;
            this.hotelDropDown.ItemBackColor = System.Drawing.Color.White;
            this.hotelDropDown.ItemBorderColor = System.Drawing.Color.White;
            this.hotelDropDown.ItemForeColor = System.Drawing.Color.DarkBlue;
            this.hotelDropDown.ItemHeight = 26;
            this.hotelDropDown.ItemHighLightColor = System.Drawing.Color.RoyalBlue;
            this.hotelDropDown.Location = new System.Drawing.Point(35, 27);
            this.hotelDropDown.Name = "hotelDropDown";
            this.hotelDropDown.Size = new System.Drawing.Size(217, 32);
            this.hotelDropDown.TabIndex = 4;
            this.hotelDropDown.Text = "Select Hotel ID";
            this.hotelDropDown.SelectedIndexChanged += new System.EventHandler(this.hotelDropDown_SelectedIndexChanged);
            // 
            // roomDropDown
            // 
            this.roomDropDown.AccessibleDescription = "";
            this.roomDropDown.BackColor = System.Drawing.SystemColors.Control;
            this.roomDropDown.BorderRadius = 1;
            this.roomDropDown.Color = System.Drawing.Color.MidnightBlue;
            this.roomDropDown.Direction = Bunifu.UI.WinForms.BunifuDropdown.Directions.Down;
            this.roomDropDown.DisabledColor = System.Drawing.Color.Gray;
            this.roomDropDown.DrawMode = System.Windows.Forms.DrawMode.OwnerDrawFixed;
            this.roomDropDown.DropdownBorderThickness = Bunifu.UI.WinForms.BunifuDropdown.BorderThickness.Thick;
            this.roomDropDown.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.roomDropDown.DropDownTextAlign = Bunifu.UI.WinForms.BunifuDropdown.TextAlign.Left;
            this.roomDropDown.FillDropDown = false;
            this.roomDropDown.FillIndicator = false;
            this.roomDropDown.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.roomDropDown.ForeColor = System.Drawing.Color.RoyalBlue;
            this.roomDropDown.FormattingEnabled = true;
            this.roomDropDown.Icon = null;
            this.roomDropDown.IndicatorColor = System.Drawing.Color.MidnightBlue;
            this.roomDropDown.IndicatorLocation = Bunifu.UI.WinForms.BunifuDropdown.Indicator.Right;
            this.roomDropDown.ItemBackColor = System.Drawing.Color.White;
            this.roomDropDown.ItemBorderColor = System.Drawing.Color.White;
            this.roomDropDown.ItemForeColor = System.Drawing.Color.DarkBlue;
            this.roomDropDown.ItemHeight = 26;
            this.roomDropDown.ItemHighLightColor = System.Drawing.Color.RoyalBlue;
            this.roomDropDown.Location = new System.Drawing.Point(35, 99);
            this.roomDropDown.Name = "roomDropDown";
            this.roomDropDown.Size = new System.Drawing.Size(217, 32);
            this.roomDropDown.TabIndex = 5;
            this.roomDropDown.Text = "Select Room ID";
            // 
            // DeleteBTN
            // 
            this.DeleteBTN.BackColor = System.Drawing.Color.Transparent;
            this.DeleteBTN.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("DeleteBTN.BackgroundImage")));
            this.DeleteBTN.ButtonText = "Delete";
            this.DeleteBTN.ButtonTextMarginLeft = 0;
            this.DeleteBTN.DisabledBorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(161)))), ((int)(((byte)(161)))), ((int)(((byte)(161)))));
            this.DeleteBTN.DisabledFillColor = System.Drawing.Color.Gray;
            this.DeleteBTN.DisabledForecolor = System.Drawing.Color.White;
            this.DeleteBTN.ForeColor = System.Drawing.Color.White;
            this.DeleteBTN.IconLeftCursor = System.Windows.Forms.Cursors.Default;
            this.DeleteBTN.IconPadding = 10;
            this.DeleteBTN.IconRightCursor = System.Windows.Forms.Cursors.Default;
            this.DeleteBTN.IdleBorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(51)))), ((int)(((byte)(122)))), ((int)(((byte)(183)))));
            this.DeleteBTN.IdleBorderRadius = 1;
            this.DeleteBTN.IdleBorderThickness = 0;
            this.DeleteBTN.IdleFillColor = System.Drawing.Color.FromArgb(((int)(((byte)(51)))), ((int)(((byte)(122)))), ((int)(((byte)(183)))));
            this.DeleteBTN.IdleIconLeftImage = null;
            this.DeleteBTN.IdleIconRightImage = null;
            this.DeleteBTN.Location = new System.Drawing.Point(35, 163);
            this.DeleteBTN.Name = "DeleteBTN";
            stateProperties1.BorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(40)))), ((int)(((byte)(96)))), ((int)(((byte)(144)))));
            stateProperties1.BorderRadius = 1;
            stateProperties1.BorderThickness = 1;
            stateProperties1.FillColor = System.Drawing.Color.FromArgb(((int)(((byte)(40)))), ((int)(((byte)(96)))), ((int)(((byte)(144)))));
            stateProperties1.IconLeftImage = null;
            stateProperties1.IconRightImage = null;
            this.DeleteBTN.onHoverState = stateProperties1;
            this.DeleteBTN.Size = new System.Drawing.Size(214, 45);
            this.DeleteBTN.TabIndex = 3;
            this.DeleteBTN.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.DeleteBTN.Click += new System.EventHandler(this.DeleteBTN_Click);
            // 
            // DeleteRoom
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(284, 261);
            this.Controls.Add(this.roomDropDown);
            this.Controls.Add(this.hotelDropDown);
            this.Controls.Add(this.DeleteBTN);
            this.Name = "DeleteRoom";
            this.Text = "DeleteRoom";
            this.ResumeLayout(false);

        }

        #endregion

        private Bunifu.UI.WinForms.BunifuButton.BunifuButton DeleteBTN;
        private Bunifu.UI.WinForms.BunifuDropdown hotelDropDown;
        private Bunifu.UI.WinForms.BunifuDropdown roomDropDown;
    }
}
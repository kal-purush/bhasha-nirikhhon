namespace Trivago.Forms
{
    partial class EditHotel
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(EditHotel));
            Bunifu.UI.WinForms.BunifuButton.BunifuButton.StateProperties stateProperties2 = new Bunifu.UI.WinForms.BunifuButton.BunifuButton.StateProperties();
            this.HotelName = new Bunifu.Framework.UI.BunifuMaterialTextbox();
            this.HotelEmail = new Bunifu.Framework.UI.BunifuMaterialTextbox();
            this.HotelNumber = new Bunifu.Framework.UI.BunifuMaterialTextbox();
            this.HotelDescription = new Bunifu.Framework.UI.BunifuMaterialTextbox();
            this.SubmitButton = new Bunifu.UI.WinForms.BunifuButton.BunifuButton();
            this.HotelRating = new Bunifu.Framework.UI.BunifuRating();
            this.HotelRatingLabel = new Bunifu.Framework.UI.BunifuCustomLabel();
            this.HotelNameDropDown = new Bunifu.UI.WinForms.BunifuDropdown();
            this.SuspendLayout();
            // 
            // HotelName
            // 
            this.HotelName.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.None;
            this.HotelName.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.None;
            this.HotelName.characterCasing = System.Windows.Forms.CharacterCasing.Normal;
            this.HotelName.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.HotelName.Font = new System.Drawing.Font("Century Gothic", 9.75F);
            this.HotelName.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.HotelName.HintForeColor = System.Drawing.Color.Silver;
            this.HotelName.HintText = "Insert New Hotel Name.";
            this.HotelName.isPassword = false;
            this.HotelName.LineFocusedColor = System.Drawing.Color.Blue;
            this.HotelName.LineIdleColor = System.Drawing.Color.Gray;
            this.HotelName.LineMouseHoverColor = System.Drawing.Color.Blue;
            this.HotelName.LineThickness = 3;
            this.HotelName.Location = new System.Drawing.Point(12, 63);
            this.HotelName.Margin = new System.Windows.Forms.Padding(4);
            this.HotelName.MaxLength = 32767;
            this.HotelName.Name = "HotelName";
            this.HotelName.Size = new System.Drawing.Size(379, 33);
            this.HotelName.TabIndex = 0;
            this.HotelName.TextAlign = System.Windows.Forms.HorizontalAlignment.Left;
            // 
            // HotelEmail
            // 
            this.HotelEmail.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.None;
            this.HotelEmail.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.None;
            this.HotelEmail.characterCasing = System.Windows.Forms.CharacterCasing.Normal;
            this.HotelEmail.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.HotelEmail.Font = new System.Drawing.Font("Century Gothic", 9.75F);
            this.HotelEmail.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.HotelEmail.HintForeColor = System.Drawing.Color.Silver;
            this.HotelEmail.HintText = "Insert New Hotel Email.";
            this.HotelEmail.isPassword = false;
            this.HotelEmail.LineFocusedColor = System.Drawing.Color.Blue;
            this.HotelEmail.LineIdleColor = System.Drawing.Color.Gray;
            this.HotelEmail.LineMouseHoverColor = System.Drawing.Color.Blue;
            this.HotelEmail.LineThickness = 3;
            this.HotelEmail.Location = new System.Drawing.Point(12, 118);
            this.HotelEmail.Margin = new System.Windows.Forms.Padding(4);
            this.HotelEmail.MaxLength = 32767;
            this.HotelEmail.Name = "HotelEmail";
            this.HotelEmail.Size = new System.Drawing.Size(379, 33);
            this.HotelEmail.TabIndex = 3;
            this.HotelEmail.TextAlign = System.Windows.Forms.HorizontalAlignment.Left;
            // 
            // HotelNumber
            // 
            this.HotelNumber.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.None;
            this.HotelNumber.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.None;
            this.HotelNumber.characterCasing = System.Windows.Forms.CharacterCasing.Normal;
            this.HotelNumber.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.HotelNumber.Font = new System.Drawing.Font("Century Gothic", 9.75F);
            this.HotelNumber.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.HotelNumber.HintForeColor = System.Drawing.Color.Silver;
            this.HotelNumber.HintText = "Insert New Hotel Number";
            this.HotelNumber.isPassword = false;
            this.HotelNumber.LineFocusedColor = System.Drawing.Color.Blue;
            this.HotelNumber.LineIdleColor = System.Drawing.Color.Gray;
            this.HotelNumber.LineMouseHoverColor = System.Drawing.Color.Blue;
            this.HotelNumber.LineThickness = 3;
            this.HotelNumber.Location = new System.Drawing.Point(12, 177);
            this.HotelNumber.Margin = new System.Windows.Forms.Padding(4);
            this.HotelNumber.MaxLength = 32767;
            this.HotelNumber.Name = "HotelNumber";
            this.HotelNumber.Size = new System.Drawing.Size(379, 33);
            this.HotelNumber.TabIndex = 4;
            this.HotelNumber.TextAlign = System.Windows.Forms.HorizontalAlignment.Left;
            // 
            // HotelDescription
            // 
            this.HotelDescription.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.None;
            this.HotelDescription.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.None;
            this.HotelDescription.characterCasing = System.Windows.Forms.CharacterCasing.Normal;
            this.HotelDescription.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.HotelDescription.Font = new System.Drawing.Font("Century Gothic", 9.75F);
            this.HotelDescription.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.HotelDescription.HintForeColor = System.Drawing.Color.Silver;
            this.HotelDescription.HintText = "Insert New Hotel Description";
            this.HotelDescription.isPassword = false;
            this.HotelDescription.LineFocusedColor = System.Drawing.Color.Blue;
            this.HotelDescription.LineIdleColor = System.Drawing.Color.Gray;
            this.HotelDescription.LineMouseHoverColor = System.Drawing.Color.Blue;
            this.HotelDescription.LineThickness = 3;
            this.HotelDescription.Location = new System.Drawing.Point(13, 232);
            this.HotelDescription.Margin = new System.Windows.Forms.Padding(4);
            this.HotelDescription.MaxLength = 32767;
            this.HotelDescription.Name = "HotelDescription";
            this.HotelDescription.Size = new System.Drawing.Size(379, 33);
            this.HotelDescription.TabIndex = 5;
            this.HotelDescription.TextAlign = System.Windows.Forms.HorizontalAlignment.Left;
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
            this.SubmitButton.Location = new System.Drawing.Point(295, 393);
            this.SubmitButton.Name = "SubmitButton";
            stateProperties2.BorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(40)))), ((int)(((byte)(96)))), ((int)(((byte)(144)))));
            stateProperties2.BorderRadius = 1;
            stateProperties2.BorderThickness = 1;
            stateProperties2.FillColor = System.Drawing.Color.FromArgb(((int)(((byte)(40)))), ((int)(((byte)(96)))), ((int)(((byte)(144)))));
            stateProperties2.IconLeftImage = null;
            stateProperties2.IconRightImage = null;
            this.SubmitButton.onHoverState = stateProperties2;
            this.SubmitButton.Size = new System.Drawing.Size(210, 45);
            this.SubmitButton.TabIndex = 6;
            this.SubmitButton.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.SubmitButton.Click += new System.EventHandler(this.SubmitButton_Click);
            // 
            // HotelRating
            // 
            this.HotelRating.BackColor = System.Drawing.Color.Transparent;
            this.HotelRating.ForeColor = System.Drawing.Color.Blue;
            this.HotelRating.Location = new System.Drawing.Point(3, 290);
            this.HotelRating.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            this.HotelRating.Name = "HotelRating";
            this.HotelRating.Size = new System.Drawing.Size(477, 82);
            this.HotelRating.TabIndex = 7;
            this.HotelRating.Value = 0;
            // 
            // HotelRatingLabel
            // 
            this.HotelRatingLabel.AutoSize = true;
            this.HotelRatingLabel.BackColor = System.Drawing.Color.Transparent;
            this.HotelRatingLabel.ForeColor = System.Drawing.SystemColors.ControlLight;
            this.HotelRatingLabel.Location = new System.Drawing.Point(154, 269);
            this.HotelRatingLabel.Name = "HotelRatingLabel";
            this.HotelRatingLabel.Size = new System.Drawing.Size(86, 17);
            this.HotelRatingLabel.TabIndex = 8;
            this.HotelRatingLabel.Text = "Hotel Rating";
            // 
            // HotelNameDropDown
            // 
            this.HotelNameDropDown.BackColor = System.Drawing.SystemColors.Control;
            this.HotelNameDropDown.BorderRadius = 1;
            this.HotelNameDropDown.Color = System.Drawing.Color.Blue;
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
            this.HotelNameDropDown.IndicatorColor = System.Drawing.Color.Blue;
            this.HotelNameDropDown.IndicatorLocation = Bunifu.UI.WinForms.BunifuDropdown.Indicator.Right;
            this.HotelNameDropDown.ItemBackColor = System.Drawing.Color.White;
            this.HotelNameDropDown.ItemBorderColor = System.Drawing.Color.White;
            this.HotelNameDropDown.ItemForeColor = System.Drawing.Color.SlateBlue;
            this.HotelNameDropDown.ItemHeight = 26;
            this.HotelNameDropDown.ItemHighLightColor = System.Drawing.Color.Lavender;
            this.HotelNameDropDown.Location = new System.Drawing.Point(12, 12);
            this.HotelNameDropDown.Name = "HotelNameDropDown";
            this.HotelNameDropDown.Size = new System.Drawing.Size(217, 32);
            this.HotelNameDropDown.TabIndex = 9;
            this.HotelNameDropDown.SelectedIndexChanged += new System.EventHandler(this.HotelNameDropDown_SelectedIndexChanged);
            // 
            // EditHotel
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackgroundImage = global::Trivago.Properties.Resources.maxresdefault;
            this.ClientSize = new System.Drawing.Size(800, 450);
            this.Controls.Add(this.HotelNameDropDown);
            this.Controls.Add(this.HotelRatingLabel);
            this.Controls.Add(this.HotelRating);
            this.Controls.Add(this.SubmitButton);
            this.Controls.Add(this.HotelDescription);
            this.Controls.Add(this.HotelNumber);
            this.Controls.Add(this.HotelEmail);
            this.Controls.Add(this.HotelName);
            this.Name = "EditHotel";
            this.Text = "EditHotel";
            this.FormClosed += new System.Windows.Forms.FormClosedEventHandler(this.EditHotel_FormClosed);
            this.Load += new System.EventHandler(this.EditHotel_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private Bunifu.Framework.UI.BunifuMaterialTextbox HotelName;
        private Bunifu.Framework.UI.BunifuMaterialTextbox HotelEmail;
        private Bunifu.Framework.UI.BunifuMaterialTextbox HotelNumber;
        private Bunifu.Framework.UI.BunifuMaterialTextbox HotelDescription;
        private Bunifu.UI.WinForms.BunifuButton.BunifuButton SubmitButton;
        private Bunifu.Framework.UI.BunifuRating HotelRating;
        private Bunifu.Framework.UI.BunifuCustomLabel HotelRatingLabel;
        private Bunifu.UI.WinForms.BunifuDropdown HotelNameDropDown;
    }
}
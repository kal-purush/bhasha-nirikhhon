namespace Trivago.Forms
{
    partial class AddHotel
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(AddHotel));
            Bunifu.UI.WinForms.BunifuButton.BunifuButton.StateProperties stateProperties1 = new Bunifu.UI.WinForms.BunifuButton.BunifuButton.StateProperties();
            this.HotelName = new Bunifu.Framework.UI.BunifuMaterialTextbox();
            this.HotelDescription = new Bunifu.Framework.UI.BunifuMaterialTextbox();
            this.HotelEmail = new Bunifu.Framework.UI.BunifuMaterialTextbox();
            this.HotelContactNumber = new Bunifu.Framework.UI.BunifuMaterialTextbox();
            this.HotelRating = new Bunifu.Framework.UI.BunifuRating();
            this.RatingLabel = new Bunifu.Framework.UI.BunifuCustomLabel();
            this.AddHotelButton = new Bunifu.UI.WinForms.BunifuButton.BunifuButton();
            this.CountryNameTextBox = new Bunifu.Framework.UI.BunifuMaterialTextbox();
            this.CityNameTextBox = new Bunifu.Framework.UI.BunifuMaterialTextbox();
            this.StreetNameTextBox = new Bunifu.Framework.UI.BunifuMaterialTextbox();
            this.HotelNameError = new System.Windows.Forms.ErrorProvider(this.components);
            this.HotelEmailError = new System.Windows.Forms.ErrorProvider(this.components);
            this.NumberError = new System.Windows.Forms.ErrorProvider(this.components);
            this.HotelRatingError = new System.Windows.Forms.ErrorProvider(this.components);
            this.CountryError = new System.Windows.Forms.ErrorProvider(this.components);
            this.CityError = new System.Windows.Forms.ErrorProvider(this.components);
            this.StreetError = new System.Windows.Forms.ErrorProvider(this.components);
            this.DescriptionError = new System.Windows.Forms.ErrorProvider(this.components);
            ((System.ComponentModel.ISupportInitialize)(this.HotelNameError)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.HotelEmailError)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.NumberError)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.HotelRatingError)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.CountryError)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.CityError)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.StreetError)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.DescriptionError)).BeginInit();
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
            this.HotelName.HintText = "Enter Hotel Name.";
            this.HotelName.isPassword = false;
            this.HotelName.LineFocusedColor = System.Drawing.Color.Blue;
            this.HotelName.LineIdleColor = System.Drawing.Color.Gray;
            this.HotelName.LineMouseHoverColor = System.Drawing.Color.Blue;
            this.HotelName.LineThickness = 3;
            this.HotelName.Location = new System.Drawing.Point(-3, 13);
            this.HotelName.Margin = new System.Windows.Forms.Padding(4);
            this.HotelName.MaxLength = 32767;
            this.HotelName.Name = "HotelName";
            this.HotelName.Size = new System.Drawing.Size(379, 33);
            this.HotelName.TabIndex = 0;
            this.HotelName.TextAlign = System.Windows.Forms.HorizontalAlignment.Left;
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
            this.HotelDescription.HintText = "Enter Hotel Description.";
            this.HotelDescription.isPassword = false;
            this.HotelDescription.LineFocusedColor = System.Drawing.Color.Blue;
            this.HotelDescription.LineIdleColor = System.Drawing.Color.Gray;
            this.HotelDescription.LineMouseHoverColor = System.Drawing.Color.Blue;
            this.HotelDescription.LineThickness = 3;
            this.HotelDescription.Location = new System.Drawing.Point(-3, 191);
            this.HotelDescription.Margin = new System.Windows.Forms.Padding(4);
            this.HotelDescription.MaxLength = 32767;
            this.HotelDescription.Name = "HotelDescription";
            this.HotelDescription.Size = new System.Drawing.Size(379, 87);
            this.HotelDescription.TabIndex = 1;
            this.HotelDescription.TextAlign = System.Windows.Forms.HorizontalAlignment.Left;
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
            this.HotelEmail.HintText = "Enter Hotel Email.";
            this.HotelEmail.isPassword = false;
            this.HotelEmail.LineFocusedColor = System.Drawing.Color.Blue;
            this.HotelEmail.LineIdleColor = System.Drawing.Color.Gray;
            this.HotelEmail.LineMouseHoverColor = System.Drawing.Color.Blue;
            this.HotelEmail.LineThickness = 3;
            this.HotelEmail.Location = new System.Drawing.Point(-3, 72);
            this.HotelEmail.Margin = new System.Windows.Forms.Padding(4);
            this.HotelEmail.MaxLength = 32767;
            this.HotelEmail.Name = "HotelEmail";
            this.HotelEmail.Size = new System.Drawing.Size(379, 33);
            this.HotelEmail.TabIndex = 2;
            this.HotelEmail.TextAlign = System.Windows.Forms.HorizontalAlignment.Left;
            // 
            // HotelContactNumber
            // 
            this.HotelContactNumber.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.None;
            this.HotelContactNumber.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.None;
            this.HotelContactNumber.characterCasing = System.Windows.Forms.CharacterCasing.Normal;
            this.HotelContactNumber.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.HotelContactNumber.Font = new System.Drawing.Font("Century Gothic", 9.75F);
            this.HotelContactNumber.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.HotelContactNumber.HintForeColor = System.Drawing.Color.Silver;
            this.HotelContactNumber.HintText = "Enter Hotel Number e.g. +2011123******";
            this.HotelContactNumber.isPassword = false;
            this.HotelContactNumber.LineFocusedColor = System.Drawing.Color.Blue;
            this.HotelContactNumber.LineIdleColor = System.Drawing.Color.Gray;
            this.HotelContactNumber.LineMouseHoverColor = System.Drawing.Color.Blue;
            this.HotelContactNumber.LineThickness = 3;
            this.HotelContactNumber.Location = new System.Drawing.Point(-3, 138);
            this.HotelContactNumber.Margin = new System.Windows.Forms.Padding(4);
            this.HotelContactNumber.MaxLength = 32767;
            this.HotelContactNumber.Name = "HotelContactNumber";
            this.HotelContactNumber.Size = new System.Drawing.Size(379, 33);
            this.HotelContactNumber.TabIndex = 3;
            this.HotelContactNumber.TextAlign = System.Windows.Forms.HorizontalAlignment.Left;
            // 
            // HotelRating
            // 
            this.HotelRating.BackColor = System.Drawing.Color.Transparent;
            this.HotelRating.ForeColor = System.Drawing.Color.DodgerBlue;
            this.HotelRating.Location = new System.Drawing.Point(-3, 308);
            this.HotelRating.Margin = new System.Windows.Forms.Padding(4, 4, 4, 4);
            this.HotelRating.Name = "HotelRating";
            this.HotelRating.Size = new System.Drawing.Size(421, 87);
            this.HotelRating.TabIndex = 4;
            this.HotelRating.Value = 0;
            // 
            // RatingLabel
            // 
            this.RatingLabel.AutoSize = true;
            this.RatingLabel.Location = new System.Drawing.Point(136, 287);
            this.RatingLabel.Name = "RatingLabel";
            this.RatingLabel.Size = new System.Drawing.Size(124, 17);
            this.RatingLabel.TabIndex = 5;
            this.RatingLabel.Text = "Enter Hotel Rating";
            // 
            // AddHotelButton
            // 
            this.AddHotelButton.BackColor = System.Drawing.Color.Transparent;
            this.AddHotelButton.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("AddHotelButton.BackgroundImage")));
            this.AddHotelButton.ButtonText = "Add Hotel";
            this.AddHotelButton.ButtonTextMarginLeft = 0;
            this.AddHotelButton.DisabledBorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(161)))), ((int)(((byte)(161)))), ((int)(((byte)(161)))));
            this.AddHotelButton.DisabledFillColor = System.Drawing.Color.Gray;
            this.AddHotelButton.DisabledForecolor = System.Drawing.Color.White;
            this.AddHotelButton.ForeColor = System.Drawing.Color.White;
            this.AddHotelButton.IconLeftCursor = System.Windows.Forms.Cursors.Default;
            this.AddHotelButton.IconPadding = 10;
            this.AddHotelButton.IconRightCursor = System.Windows.Forms.Cursors.Default;
            this.AddHotelButton.IdleBorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(51)))), ((int)(((byte)(122)))), ((int)(((byte)(183)))));
            this.AddHotelButton.IdleBorderRadius = 1;
            this.AddHotelButton.IdleBorderThickness = 0;
            this.AddHotelButton.IdleFillColor = System.Drawing.Color.FromArgb(((int)(((byte)(51)))), ((int)(((byte)(122)))), ((int)(((byte)(183)))));
            this.AddHotelButton.IdleIconLeftImage = null;
            this.AddHotelButton.IdleIconRightImage = null;
            this.AddHotelButton.Location = new System.Drawing.Point(386, 402);
            this.AddHotelButton.Name = "AddHotelButton";
            stateProperties1.BorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(40)))), ((int)(((byte)(96)))), ((int)(((byte)(144)))));
            stateProperties1.BorderRadius = 1;
            stateProperties1.BorderThickness = 1;
            stateProperties1.FillColor = System.Drawing.Color.FromArgb(((int)(((byte)(40)))), ((int)(((byte)(96)))), ((int)(((byte)(144)))));
            stateProperties1.IconLeftImage = null;
            stateProperties1.IconRightImage = null;
            this.AddHotelButton.onHoverState = stateProperties1;
            this.AddHotelButton.Size = new System.Drawing.Size(210, 45);
            this.AddHotelButton.TabIndex = 6;
            this.AddHotelButton.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.AddHotelButton.Click += new System.EventHandler(this.AddHotelButton_Click);
            // 
            // CountryNameTextBox
            // 
            this.CountryNameTextBox.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.None;
            this.CountryNameTextBox.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.None;
            this.CountryNameTextBox.characterCasing = System.Windows.Forms.CharacterCasing.Normal;
            this.CountryNameTextBox.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.CountryNameTextBox.Font = new System.Drawing.Font("Century Gothic", 9.75F);
            this.CountryNameTextBox.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.CountryNameTextBox.HintForeColor = System.Drawing.Color.Silver;
            this.CountryNameTextBox.HintText = "Enter Country Name e.g. Spain.";
            this.CountryNameTextBox.isPassword = false;
            this.CountryNameTextBox.LineFocusedColor = System.Drawing.Color.Blue;
            this.CountryNameTextBox.LineIdleColor = System.Drawing.Color.Gray;
            this.CountryNameTextBox.LineMouseHoverColor = System.Drawing.Color.Blue;
            this.CountryNameTextBox.LineThickness = 3;
            this.CountryNameTextBox.Location = new System.Drawing.Point(465, 13);
            this.CountryNameTextBox.Margin = new System.Windows.Forms.Padding(4);
            this.CountryNameTextBox.MaxLength = 32767;
            this.CountryNameTextBox.Name = "CountryNameTextBox";
            this.CountryNameTextBox.Size = new System.Drawing.Size(359, 33);
            this.CountryNameTextBox.TabIndex = 10;
            this.CountryNameTextBox.TextAlign = System.Windows.Forms.HorizontalAlignment.Left;
            // 
            // CityNameTextBox
            // 
            this.CityNameTextBox.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.None;
            this.CityNameTextBox.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.None;
            this.CityNameTextBox.characterCasing = System.Windows.Forms.CharacterCasing.Normal;
            this.CityNameTextBox.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.CityNameTextBox.Font = new System.Drawing.Font("Century Gothic", 9.75F);
            this.CityNameTextBox.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.CityNameTextBox.HintForeColor = System.Drawing.Color.Silver;
            this.CityNameTextBox.HintText = "Enter City Name e.g. Madrid";
            this.CityNameTextBox.isPassword = false;
            this.CityNameTextBox.LineFocusedColor = System.Drawing.Color.Blue;
            this.CityNameTextBox.LineIdleColor = System.Drawing.Color.Gray;
            this.CityNameTextBox.LineMouseHoverColor = System.Drawing.Color.Blue;
            this.CityNameTextBox.LineThickness = 3;
            this.CityNameTextBox.Location = new System.Drawing.Point(465, 72);
            this.CityNameTextBox.Margin = new System.Windows.Forms.Padding(4);
            this.CityNameTextBox.MaxLength = 32767;
            this.CityNameTextBox.Name = "CityNameTextBox";
            this.CityNameTextBox.Size = new System.Drawing.Size(359, 33);
            this.CityNameTextBox.TabIndex = 11;
            this.CityNameTextBox.TextAlign = System.Windows.Forms.HorizontalAlignment.Left;
            // 
            // StreetNameTextBox
            // 
            this.StreetNameTextBox.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.None;
            this.StreetNameTextBox.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.None;
            this.StreetNameTextBox.characterCasing = System.Windows.Forms.CharacterCasing.Normal;
            this.StreetNameTextBox.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.StreetNameTextBox.Font = new System.Drawing.Font("Century Gothic", 9.75F);
            this.StreetNameTextBox.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.StreetNameTextBox.HintForeColor = System.Drawing.Color.Silver;
            this.StreetNameTextBox.HintText = "Enter Street Name e.g.Puerta del Sol";
            this.StreetNameTextBox.isPassword = false;
            this.StreetNameTextBox.LineFocusedColor = System.Drawing.Color.Blue;
            this.StreetNameTextBox.LineIdleColor = System.Drawing.Color.Gray;
            this.StreetNameTextBox.LineMouseHoverColor = System.Drawing.Color.Blue;
            this.StreetNameTextBox.LineThickness = 3;
            this.StreetNameTextBox.Location = new System.Drawing.Point(465, 137);
            this.StreetNameTextBox.Margin = new System.Windows.Forms.Padding(4);
            this.StreetNameTextBox.MaxLength = 32767;
            this.StreetNameTextBox.Name = "StreetNameTextBox";
            this.StreetNameTextBox.Size = new System.Drawing.Size(379, 33);
            this.StreetNameTextBox.TabIndex = 12;
            this.StreetNameTextBox.TextAlign = System.Windows.Forms.HorizontalAlignment.Left;
            // 
            // HotelNameError
            // 
            this.HotelNameError.ContainerControl = this;
            // 
            // HotelEmailError
            // 
            this.HotelEmailError.ContainerControl = this;
            // 
            // NumberError
            // 
            this.NumberError.ContainerControl = this;
            // 
            // HotelRatingError
            // 
            this.HotelRatingError.ContainerControl = this;
            // 
            // CountryError
            // 
            this.CountryError.ContainerControl = this;
            // 
            // CityError
            // 
            this.CityError.ContainerControl = this;
            // 
            // StreetError
            // 
            this.StreetError.ContainerControl = this;
            // 
            // DescriptionError
            // 
            this.DescriptionError.ContainerControl = this;
            // 
            // AddHotel
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.Color.Snow;
            this.BackgroundImage = global::Trivago.Properties.Resources.maxresdefault;
            this.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Center;
            this.ClientSize = new System.Drawing.Size(988, 450);
            this.Controls.Add(this.StreetNameTextBox);
            this.Controls.Add(this.CityNameTextBox);
            this.Controls.Add(this.CountryNameTextBox);
            this.Controls.Add(this.AddHotelButton);
            this.Controls.Add(this.RatingLabel);
            this.Controls.Add(this.HotelRating);
            this.Controls.Add(this.HotelContactNumber);
            this.Controls.Add(this.HotelEmail);
            this.Controls.Add(this.HotelDescription);
            this.Controls.Add(this.HotelName);
            this.DoubleBuffered = true;
            this.Name = "AddHotel";
            this.Text = "AddHotel";
            this.Load += new System.EventHandler(this.AddHotel_Load);
            ((System.ComponentModel.ISupportInitialize)(this.HotelNameError)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.HotelEmailError)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.NumberError)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.HotelRatingError)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.CountryError)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.CityError)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.StreetError)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.DescriptionError)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private Bunifu.Framework.UI.BunifuMaterialTextbox HotelName;
        private Bunifu.Framework.UI.BunifuMaterialTextbox HotelDescription;
        private Bunifu.Framework.UI.BunifuMaterialTextbox HotelEmail;
        private Bunifu.Framework.UI.BunifuMaterialTextbox HotelContactNumber;
        private Bunifu.Framework.UI.BunifuRating HotelRating;
        private Bunifu.Framework.UI.BunifuCustomLabel RatingLabel;
        private Bunifu.UI.WinForms.BunifuButton.BunifuButton AddHotelButton;
        private Bunifu.Framework.UI.BunifuMaterialTextbox CountryNameTextBox;
        private Bunifu.Framework.UI.BunifuMaterialTextbox CityNameTextBox;
        private Bunifu.Framework.UI.BunifuMaterialTextbox StreetNameTextBox;
        private System.Windows.Forms.ErrorProvider HotelNameError;
        private System.Windows.Forms.ErrorProvider HotelEmailError;
        private System.Windows.Forms.ErrorProvider NumberError;
        private System.Windows.Forms.ErrorProvider HotelRatingError;
        private System.Windows.Forms.ErrorProvider CountryError;
        private System.Windows.Forms.ErrorProvider CityError;
        private System.Windows.Forms.ErrorProvider StreetError;
        private System.Windows.Forms.ErrorProvider DescriptionError;
    }
}
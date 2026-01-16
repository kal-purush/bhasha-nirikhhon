namespace Trivago.Forms
{
    partial class DeleteWebsite
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(DeleteWebsite));
            this.delete = new Bunifu.Framework.UI.BunifuThinButton2();
            this.website = new Bunifu.UI.WinForms.BunifuDropdown();
            this.SuspendLayout();
            // 
            // delete
            // 
            this.delete.ActiveBorderThickness = 1;
            this.delete.ActiveCornerRadius = 20;
            this.delete.ActiveFillColor = System.Drawing.Color.Purple;
            this.delete.ActiveForecolor = System.Drawing.Color.White;
            this.delete.ActiveLineColor = System.Drawing.Color.Purple;
            this.delete.BackColor = System.Drawing.SystemColors.Control;
            this.delete.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("delete.BackgroundImage")));
            this.delete.ButtonText = "Delete";
            this.delete.Cursor = System.Windows.Forms.Cursors.Hand;
            this.delete.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.delete.ForeColor = System.Drawing.Color.Purple;
            this.delete.IdleBorderThickness = 1;
            this.delete.IdleCornerRadius = 20;
            this.delete.IdleFillColor = System.Drawing.Color.White;
            this.delete.IdleForecolor = System.Drawing.Color.Purple;
            this.delete.IdleLineColor = System.Drawing.Color.Purple;
            this.delete.Location = new System.Drawing.Point(78, 173);
            this.delete.Margin = new System.Windows.Forms.Padding(5);
            this.delete.Name = "delete";
            this.delete.Size = new System.Drawing.Size(181, 41);
            this.delete.TabIndex = 2;
            this.delete.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.delete.Click += new System.EventHandler(this.delete_Click);
            // 
            // website
            // 
            this.website.BackColor = System.Drawing.SystemColors.Control;
            this.website.BorderRadius = 1;
            this.website.Direction = Bunifu.UI.WinForms.BunifuDropdown.Directions.Down;
            this.website.DisabledColor = System.Drawing.Color.Gray;
            this.website.DrawMode = System.Windows.Forms.DrawMode.OwnerDrawFixed;
            this.website.DropdownBorderThickness = Bunifu.UI.WinForms.BunifuDropdown.BorderThickness.Thick;
            this.website.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.website.DropDownTextAlign = Bunifu.UI.WinForms.BunifuDropdown.TextAlign.Center;
            this.website.FillDropDown = false;
            this.website.FillIndicator = false;
            this.website.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.website.Font = new System.Drawing.Font("Times New Roman", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.website.ForeColor = System.Drawing.Color.Purple;
            this.website.FormattingEnabled = true;
            this.website.Icon = null;
            this.website.IndicatorColor = System.Drawing.Color.Purple;
            this.website.IndicatorLocation = Bunifu.UI.WinForms.BunifuDropdown.Indicator.None;
            this.website.ItemBackColor = System.Drawing.Color.White;
            this.website.ItemBorderColor = System.Drawing.Color.White;
            this.website.ItemForeColor = System.Drawing.Color.Purple;
            this.website.ItemHeight = 26;
            this.website.ItemHighLightColor = System.Drawing.Color.Thistle;
            this.website.Location = new System.Drawing.Point(62, 118);
            this.website.Name = "website";
            this.website.Size = new System.Drawing.Size(217, 32);
            this.website.TabIndex = 3;
            this.website.Text = "Website ID";
            this.website.SelectedIndexChanged += new System.EventHandler(this.website_SelectedIndexChanged);
            // 
            // DeleteWebsite
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(350, 347);
            this.Controls.Add(this.website);
            this.Controls.Add(this.delete);
            this.Name = "DeleteWebsite";
            this.Text = "DeleteWebsite";
            this.ResumeLayout(false);

        }

        #endregion

        private Bunifu.Framework.UI.BunifuThinButton2 delete;
        private Bunifu.UI.WinForms.BunifuDropdown website;
    }
}
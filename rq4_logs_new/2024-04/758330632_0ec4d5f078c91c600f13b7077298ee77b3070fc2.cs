namespace QuanLyTraoDoiHang
{
    partial class FPurchaseDone
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(FPurchaseDone));
            btnBack = new RJControls.RButton();
            label1 = new Label();
            lblTotalPrice = new Label();
            label3 = new Label();
            btnHome = new RJControls.RButton();
            btnMyPurchases = new RJControls.RButton();
            btnCart = new RJControls.RButton();
            SuspendLayout();
            // 
            // btnBack
            // 
            btnBack.BackColor = Color.FromArgb(101, 183, 65);
            btnBack.BackgroundColor = Color.FromArgb(101, 183, 65);
            btnBack.BorderColor = Color.FromArgb(101, 183, 65);
            btnBack.BorderRadius = 20;
            btnBack.BorderSize = 0;
            btnBack.FlatAppearance.BorderSize = 0;
            btnBack.FlatStyle = FlatStyle.Flat;
            btnBack.ForeColor = Color.White;
            btnBack.Image = (Image)resources.GetObject("btnBack.Image");
            btnBack.Location = new Point(2, 0);
            btnBack.Name = "btnBack";
            btnBack.Size = new Size(35, 28);
            btnBack.TabIndex = 0;
            btnBack.TextColor = Color.White;
            btnBack.UseVisualStyleBackColor = false;
            btnBack.Click += btnBack_Click;
            // 
            // label1
            // 
            label1.Font = new Font("Segoe UI Semibold", 12F, FontStyle.Bold, GraphicsUnit.Point);
            label1.ForeColor = Color.White;
            label1.Image = (Image)resources.GetObject("label1.Image");
            label1.ImageAlign = ContentAlignment.TopLeft;
            label1.Location = new Point(104, 102);
            label1.Name = "label1";
            label1.Size = new Size(100, 27);
            label1.TabIndex = 1;
            label1.Text = "You paid";
            label1.TextAlign = ContentAlignment.MiddleRight;
            // 
            // lblTotalPrice
            // 
            lblTotalPrice.AutoSize = true;
            lblTotalPrice.Font = new Font("Segoe UI Semibold", 12F, FontStyle.Bold, GraphicsUnit.Point);
            lblTotalPrice.ForeColor = Color.White;
            lblTotalPrice.Location = new Point(210, 105);
            lblTotalPrice.Name = "lblTotalPrice";
            lblTotalPrice.Size = new Size(46, 21);
            lblTotalPrice.TabIndex = 3;
            lblTotalPrice.Text = "2000";
            // 
            // label3
            // 
            label3.Font = new Font("Segoe UI", 12F, FontStyle.Underline, GraphicsUnit.Point);
            label3.ForeColor = Color.White;
            label3.Location = new Point(198, 105);
            label3.Name = "label3";
            label3.Size = new Size(17, 19);
            label3.TabIndex = 4;
            label3.Text = "đ";
            // 
            // btnHome
            // 
            btnHome.BackColor = Color.FromArgb(101, 183, 65);
            btnHome.BackgroundColor = Color.FromArgb(101, 183, 65);
            btnHome.BorderColor = Color.White;
            btnHome.BorderRadius = 10;
            btnHome.BorderSize = 1;
            btnHome.FlatAppearance.BorderSize = 0;
            btnHome.FlatStyle = FlatStyle.Flat;
            btnHome.Font = new Font("Segoe UI Semibold", 12F, FontStyle.Bold, GraphicsUnit.Point);
            btnHome.ForeColor = Color.White;
            btnHome.Location = new Point(12, 207);
            btnHome.Name = "btnHome";
            btnHome.Size = new Size(177, 40);
            btnHome.TabIndex = 9;
            btnHome.Text = "Home";
            btnHome.TextColor = Color.White;
            btnHome.UseVisualStyleBackColor = false;
            btnHome.Click += btnHome_Click;
            // 
            // btnMyPurchases
            // 
            btnMyPurchases.BackColor = Color.FromArgb(101, 183, 65);
            btnMyPurchases.BackgroundColor = Color.FromArgb(101, 183, 65);
            btnMyPurchases.BorderColor = Color.White;
            btnMyPurchases.BorderRadius = 10;
            btnMyPurchases.BorderSize = 1;
            btnMyPurchases.FlatAppearance.BorderSize = 0;
            btnMyPurchases.FlatStyle = FlatStyle.Flat;
            btnMyPurchases.Font = new Font("Segoe UI Semibold", 12F, FontStyle.Bold, GraphicsUnit.Point);
            btnMyPurchases.ForeColor = Color.White;
            btnMyPurchases.Location = new Point(195, 207);
            btnMyPurchases.Name = "btnMyPurchases";
            btnMyPurchases.Size = new Size(177, 40);
            btnMyPurchases.TabIndex = 10;
            btnMyPurchases.Text = "My purchases";
            btnMyPurchases.TextColor = Color.White;
            btnMyPurchases.UseVisualStyleBackColor = false;
            btnMyPurchases.Click += btnMyPurchases_Click;
            // 
            // btnCart
            // 
            btnCart.BackColor = Color.FromArgb(101, 183, 65);
            btnCart.BackgroundColor = Color.FromArgb(101, 183, 65);
            btnCart.BorderColor = Color.White;
            btnCart.BorderRadius = 10;
            btnCart.BorderSize = 0;
            btnCart.FlatAppearance.BorderSize = 0;
            btnCart.FlatStyle = FlatStyle.Flat;
            btnCart.Font = new Font("Segoe UI Semibold", 12F, FontStyle.Bold, GraphicsUnit.Point);
            btnCart.ForeColor = Color.White;
            btnCart.Image = (Image)resources.GetObject("btnCart.Image");
            btnCart.Location = new Point(329, 0);
            btnCart.Name = "btnCart";
            btnCart.Size = new Size(43, 40);
            btnCart.TabIndex = 11;
            btnCart.TextColor = Color.White;
            btnCart.UseVisualStyleBackColor = false;
            btnCart.Click += btnCart_Click;
            // 
            // FPurchaseDone
            // 
            AutoScaleDimensions = new SizeF(7F, 15F);
            AutoScaleMode = AutoScaleMode.Font;
            BackColor = Color.FromArgb(101, 183, 65);
            ClientSize = new Size(384, 261);
            Controls.Add(btnCart);
            Controls.Add(btnMyPurchases);
            Controls.Add(btnHome);
            Controls.Add(label3);
            Controls.Add(lblTotalPrice);
            Controls.Add(label1);
            Controls.Add(btnBack);
            FormBorderStyle = FormBorderStyle.None;
            Name = "FPurchaseDone";
            StartPosition = FormStartPosition.CenterScreen;
            Text = "FPurchaseDone";
            ResumeLayout(false);
            PerformLayout();
        }

        #endregion

        private RJControls.RButton btnBack;
        private Label label1;
        private Label lblTotalPrice;
        private Label label3;
        private RJControls.RButton btnHome;
        private RJControls.RButton btnMyPurchases;
        private RJControls.RButton btnCart;
    }
}
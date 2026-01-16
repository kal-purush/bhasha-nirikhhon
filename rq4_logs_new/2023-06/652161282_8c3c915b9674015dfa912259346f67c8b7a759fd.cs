namespace Recipe_Organizer_PRN211.Authentication
{
    partial class AdminPage
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
            label5 = new Label();
            flowLayoutPanel1 = new FlowLayoutPanel();
            panel3 = new Panel();
            btnAdminProfile = new Button();
            panel2 = new Panel();
            btnUserList = new Button();
            panel6 = new Panel();
            panel7 = new Panel();
            btnLogout = new Button();
            panel4 = new Panel();
            btnCategorySetting = new Button();
            panel5 = new Panel();
            Pn_body = new Panel();
            flowLayoutPanel1.SuspendLayout();
            panel3.SuspendLayout();
            panel2.SuspendLayout();
            panel7.SuspendLayout();
            panel4.SuspendLayout();
            SuspendLayout();
            // 
            // label5
            // 
            label5.AutoSize = true;
            label5.Font = new Font("Montserrat Black", 20.25F, FontStyle.Bold, GraphicsUnit.Point);
            label5.ForeColor = Color.FromArgb(192, 0, 0);
            label5.Location = new Point(593, 9);
            label5.Name = "label5";
            label5.Size = new Size(195, 37);
            label5.TabIndex = 11;
            label5.Text = "Admin page";
            // 
            // flowLayoutPanel1
            // 
            flowLayoutPanel1.Controls.Add(panel3);
            flowLayoutPanel1.Controls.Add(panel2);
            flowLayoutPanel1.Controls.Add(panel6);
            flowLayoutPanel1.Controls.Add(panel7);
            flowLayoutPanel1.Location = new Point(12, 12);
            flowLayoutPanel1.Name = "flowLayoutPanel1";
            flowLayoutPanel1.Size = new Size(283, 426);
            flowLayoutPanel1.TabIndex = 21;
            // 
            // panel3
            // 
            panel3.Controls.Add(btnAdminProfile);
            panel3.Location = new Point(3, 3);
            panel3.Name = "panel3";
            panel3.Size = new Size(280, 56);
            panel3.TabIndex = 1;
            // 
            // btnAdminProfile
            // 
            btnAdminProfile.Dock = DockStyle.Fill;
            btnAdminProfile.Font = new Font("Montserrat Black", 20.25F, FontStyle.Bold, GraphicsUnit.Point);
            btnAdminProfile.ForeColor = Color.Coral;
            btnAdminProfile.Location = new Point(0, 0);
            btnAdminProfile.Name = "btnAdminProfile";
            btnAdminProfile.Size = new Size(280, 56);
            btnAdminProfile.TabIndex = 0;
            btnAdminProfile.Text = "Admin profile";
            btnAdminProfile.UseVisualStyleBackColor = true;
            // 
            // panel2
            // 
            panel2.Controls.Add(btnUserList);
            panel2.Location = new Point(3, 65);
            panel2.Name = "panel2";
            panel2.Size = new Size(280, 56);
            panel2.TabIndex = 0;
            // 
            // btnUserList
            // 
            btnUserList.Dock = DockStyle.Fill;
            btnUserList.Font = new Font("Montserrat Black", 20.25F, FontStyle.Bold, GraphicsUnit.Point);
            btnUserList.ForeColor = Color.Coral;
            btnUserList.Location = new Point(0, 0);
            btnUserList.Name = "btnUserList";
            btnUserList.Size = new Size(280, 56);
            btnUserList.TabIndex = 1;
            btnUserList.Text = "User List";
            btnUserList.UseVisualStyleBackColor = true;
            btnUserList.Click += btnUserList_Click;
            // 
            // panel6
            // 
            panel6.Location = new Point(3, 127);
            panel6.Name = "panel6";
            panel6.Size = new Size(193, 56);
            panel6.TabIndex = 2;
            // 
            // panel7
            // 
            panel7.Controls.Add(btnLogout);
            panel7.Location = new Point(3, 189);
            panel7.Name = "panel7";
            panel7.Size = new Size(280, 56);
            panel7.TabIndex = 2;
            // 
            // btnLogout
            // 
            btnLogout.Dock = DockStyle.Fill;
            btnLogout.Font = new Font("Montserrat Black", 20.25F, FontStyle.Bold, GraphicsUnit.Point);
            btnLogout.ForeColor = Color.Coral;
            btnLogout.Location = new Point(0, 0);
            btnLogout.Name = "btnLogout";
            btnLogout.Size = new Size(280, 56);
            btnLogout.TabIndex = 1;
            btnLogout.Text = "Logout";
            btnLogout.UseVisualStyleBackColor = true;
            // 
            // panel4
            // 
            panel4.Controls.Add(btnCategorySetting);
            panel4.Location = new Point(12, 139);
            panel4.Name = "panel4";
            panel4.Size = new Size(283, 56);
            panel4.TabIndex = 1;
            // 
            // btnCategorySetting
            // 
            btnCategorySetting.Dock = DockStyle.Fill;
            btnCategorySetting.Font = new Font("Montserrat Black", 20.25F, FontStyle.Bold, GraphicsUnit.Point);
            btnCategorySetting.ForeColor = Color.Coral;
            btnCategorySetting.Location = new Point(0, 0);
            btnCategorySetting.Name = "btnCategorySetting";
            btnCategorySetting.Size = new Size(283, 56);
            btnCategorySetting.TabIndex = 1;
            btnCategorySetting.Text = "Category setting";
            btnCategorySetting.UseVisualStyleBackColor = true;
            // 
            // panel5
            // 
            panel5.Location = new Point(12, 206);
            panel5.Name = "panel5";
            panel5.Size = new Size(193, 56);
            panel5.TabIndex = 2;
            // 
            // Pn_body
            // 
            Pn_body.Location = new Point(317, 59);
            Pn_body.Name = "Pn_body";
            Pn_body.Size = new Size(471, 379);
            Pn_body.TabIndex = 22;
            // 
            // AdminPage
            // 
            AutoScaleDimensions = new SizeF(7F, 15F);
            AutoScaleMode = AutoScaleMode.Font;
            ClientSize = new Size(800, 450);
            Controls.Add(Pn_body);
            Controls.Add(panel4);
            Controls.Add(flowLayoutPanel1);
            Controls.Add(panel5);
            Controls.Add(label5);
            Name = "AdminPage";
            Text = "AdminPage";
            Load += AdminPage_Load;
            flowLayoutPanel1.ResumeLayout(false);
            panel3.ResumeLayout(false);
            panel2.ResumeLayout(false);
            panel7.ResumeLayout(false);
            panel4.ResumeLayout(false);
            ResumeLayout(false);
            PerformLayout();
        }

        #endregion

        private Label label5;
        private FlowLayoutPanel flowLayoutPanel1;
        private Panel panel3;
        private Panel panel2;
        private Panel panel6;
        private Panel panel7;
        private Panel panel4;
        private Panel panel5;
        private Button btnAdminProfile;
        private Button btnUserList;
        private Button btnLogout;
        private Button btnCategorySetting;
        private Panel Pn_body;
    }
}
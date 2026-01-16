namespace Recipe_Organizer_PRN211.Manage
{
    partial class PendingRecipe
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
            btnReject = new Button();
            btnApprove = new Button();
            txtDescription = new TextBox();
            txtTitle = new TextBox();
            label3 = new Label();
            label2 = new Label();
            Emtpty = new Label();
            btnRefresh = new Button();
            btnSearch = new Button();
            txtSearch = new TextBox();
            dgvPendingRecipe = new DataGridView();
            label5 = new Label();
            dateCreate = new DateTimePicker();
            pictureBox1 = new PictureBox();
            ((System.ComponentModel.ISupportInitialize)dgvPendingRecipe).BeginInit();
            ((System.ComponentModel.ISupportInitialize)pictureBox1).BeginInit();
            SuspendLayout();
            // 
            // btnReject
            // 
            btnReject.BackColor = Color.Red;
            btnReject.Font = new Font("Montserrat", 8.999999F, FontStyle.Bold, GraphicsUnit.Point);
            btnReject.ForeColor = Color.White;
            btnReject.Location = new Point(654, 239);
            btnReject.Name = "btnReject";
            btnReject.Size = new Size(141, 50);
            btnReject.TabIndex = 56;
            btnReject.Text = "Reject";
            btnReject.UseVisualStyleBackColor = false;
            btnReject.Click += btnReject_Click;
            // 
            // btnApprove
            // 
            btnApprove.BackColor = Color.Lime;
            btnApprove.Font = new Font("Montserrat", 8.999999F, FontStyle.Bold, GraphicsUnit.Point);
            btnApprove.Location = new Point(449, 239);
            btnApprove.Name = "btnApprove";
            btnApprove.Size = new Size(141, 50);
            btnApprove.TabIndex = 55;
            btnApprove.Text = "Approve";
            btnApprove.UseVisualStyleBackColor = false;
            btnApprove.Click += btnApprove_Click;
            // 
            // txtDescription
            // 
            txtDescription.Location = new Point(120, 325);
            txtDescription.Multiline = true;
            txtDescription.Name = "txtDescription";
            txtDescription.Size = new Size(229, 23);
            txtDescription.TabIndex = 48;
            // 
            // txtTitle
            // 
            txtTitle.Location = new Point(120, 288);
            txtTitle.Name = "txtTitle";
            txtTitle.Size = new Size(229, 23);
            txtTitle.TabIndex = 47;
            // 
            // label3
            // 
            label3.AutoSize = true;
            label3.Font = new Font("Montserrat SemiBold", 9.749999F, FontStyle.Bold, GraphicsUnit.Point);
            label3.Location = new Point(33, 253);
            label3.Name = "label3";
            label3.Size = new Size(87, 18);
            label3.TabIndex = 44;
            label3.Text = "Create date";
            // 
            // label2
            // 
            label2.AutoSize = true;
            label2.Font = new Font("Montserrat SemiBold", 9.749999F, FontStyle.Bold, GraphicsUnit.Point);
            label2.Location = new Point(33, 326);
            label2.Name = "label2";
            label2.Size = new Size(87, 18);
            label2.TabIndex = 43;
            label2.Text = "Description";
            // 
            // Emtpty
            // 
            Emtpty.AutoSize = true;
            Emtpty.Font = new Font("Montserrat SemiBold", 9.749999F, FontStyle.Bold, GraphicsUnit.Point);
            Emtpty.Location = new Point(33, 288);
            Emtpty.Name = "Emtpty";
            Emtpty.Size = new Size(38, 18);
            Emtpty.TabIndex = 42;
            Emtpty.Text = "Title";
            // 
            // btnRefresh
            // 
            btnRefresh.Font = new Font("Montserrat Medium", 8.249999F, FontStyle.Bold, GraphicsUnit.Point);
            btnRefresh.Location = new Point(418, 34);
            btnRefresh.Name = "btnRefresh";
            btnRefresh.Size = new Size(75, 23);
            btnRefresh.TabIndex = 41;
            btnRefresh.Text = "Refresh";
            btnRefresh.UseVisualStyleBackColor = true;
            btnRefresh.Click += btnRefresh_Click;
            // 
            // btnSearch
            // 
            btnSearch.Font = new Font("Montserrat Medium", 8.249999F, FontStyle.Bold, GraphicsUnit.Point);
            btnSearch.Location = new Point(337, 34);
            btnSearch.Name = "btnSearch";
            btnSearch.Size = new Size(75, 23);
            btnSearch.TabIndex = 40;
            btnSearch.Text = "Search";
            btnSearch.UseVisualStyleBackColor = true;
            btnSearch.Click += btnSearch_Click;
            // 
            // txtSearch
            // 
            txtSearch.Location = new Point(26, 34);
            txtSearch.Name = "txtSearch";
            txtSearch.Size = new Size(305, 23);
            txtSearch.TabIndex = 39;
            // 
            // dgvPendingRecipe
            // 
            dgvPendingRecipe.ColumnHeadersHeightSizeMode = DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            dgvPendingRecipe.Location = new Point(26, 74);
            dgvPendingRecipe.Name = "dgvPendingRecipe";
            dgvPendingRecipe.RowTemplate.Height = 25;
            dgvPendingRecipe.Size = new Size(769, 145);
            dgvPendingRecipe.TabIndex = 38;
            dgvPendingRecipe.CellDoubleClick += dgvPendingRecipe_CellDoubleClick;
            // 
            // label5
            // 
            label5.AutoSize = true;
            label5.Font = new Font("Montserrat Black", 20.25F, FontStyle.Bold, GraphicsUnit.Point);
            label5.ForeColor = Color.FromArgb(192, 0, 0);
            label5.Location = new Point(554, 20);
            label5.Name = "label5";
            label5.Size = new Size(241, 37);
            label5.TabIndex = 37;
            label5.Text = "Pending recipe";
            // 
            // dateCreate
            // 
            dateCreate.Location = new Point(120, 249);
            dateCreate.Name = "dateCreate";
            dateCreate.Size = new Size(229, 23);
            dateCreate.TabIndex = 59;
            // 
            // pictureBox1
            // 
            pictureBox1.Image = Properties.Resources.raspberry_good_fruit_plant_4k_um_1600x900;
            pictureBox1.Location = new Point(449, 314);
            pictureBox1.Name = "pictureBox1";
            pictureBox1.Size = new Size(346, 139);
            pictureBox1.SizeMode = PictureBoxSizeMode.Zoom;
            pictureBox1.TabIndex = 60;
            pictureBox1.TabStop = false;
            // 
            // PendingRecipe
            // 
            AutoScaleDimensions = new SizeF(7F, 15F);
            AutoScaleMode = AutoScaleMode.Font;
            ClientSize = new Size(843, 474);
            Controls.Add(pictureBox1);
            Controls.Add(dateCreate);
            Controls.Add(btnReject);
            Controls.Add(btnApprove);
            Controls.Add(txtDescription);
            Controls.Add(txtTitle);
            Controls.Add(label3);
            Controls.Add(label2);
            Controls.Add(Emtpty);
            Controls.Add(btnRefresh);
            Controls.Add(btnSearch);
            Controls.Add(txtSearch);
            Controls.Add(dgvPendingRecipe);
            Controls.Add(label5);
            Name = "PendingRecipe";
            Text = "PendingRecipe";
            ((System.ComponentModel.ISupportInitialize)dgvPendingRecipe).EndInit();
            ((System.ComponentModel.ISupportInitialize)pictureBox1).EndInit();
            ResumeLayout(false);
            PerformLayout();
        }

        #endregion

        private ComboBox cbRole;
        private DateTimePicker dateBirthday;
        private Button btnReject;
        private Button btnDelete;
        private Button btnApprove;
        private Button btnCreate;
        private Label label8;
        private Label label7;
        private TextBox txtPassword;
        private TextBox txtEmail;
        private TextBox txtLastname;
        private TextBox txtDescription;
        private TextBox txtTitle;
        private Label label6;
        private Label label4;
        private Label label3;
        private Label label2;
        private Label Emtpty;
        private Button btnRefresh;
        private Button btnSearch;
        private TextBox txtSearch;
        private DataGridView dgvPendingRecipe;
        private Label label5;
        private DateTimePicker dateCreate;
        private PictureBox pictureBox1;
    }
}
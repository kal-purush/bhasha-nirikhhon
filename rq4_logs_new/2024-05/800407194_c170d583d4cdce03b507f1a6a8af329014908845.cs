namespace LoginForm.DashboardForm
{
    partial class HomeForm
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(HomeForm));
            pictureBox1 = new PictureBox();
            PvPButton = new Button();
            PvEButton = new Button();
            label1 = new Label();
            label2 = new Label();
            ((System.ComponentModel.ISupportInitialize)pictureBox1).BeginInit();
            SuspendLayout();
            // 
            // pictureBox1
            // 
            pictureBox1.Image = (Image)resources.GetObject("pictureBox1.Image");
            pictureBox1.Location = new Point(86, 115);
            pictureBox1.Name = "pictureBox1";
            pictureBox1.Size = new Size(435, 441);
            pictureBox1.SizeMode = PictureBoxSizeMode.Zoom;
            pictureBox1.TabIndex = 0;
            pictureBox1.TabStop = false;
            // 
            // PvPButton
            // 
            PvPButton.BackgroundImage = (Image)resources.GetObject("PvPButton.BackgroundImage");
            PvPButton.BackgroundImageLayout = ImageLayout.Stretch;
            PvPButton.FlatAppearance.BorderSize = 0;
            PvPButton.FlatStyle = FlatStyle.Flat;
            PvPButton.Font = new Font("Microsoft Sans Serif", 22.2F, FontStyle.Bold, GraphicsUnit.Point, 0);
            PvPButton.Location = new Point(574, 449);
            PvPButton.Name = "PvPButton";
            PvPButton.Size = new Size(254, 78);
            PvPButton.TabIndex = 13;
            PvPButton.Text = "⚔ PvP ⚔️";
            PvPButton.UseVisualStyleBackColor = true;
            PvPButton.Click += PvPButton_Click;
            // 
            // PvEButton
            // 
            PvEButton.BackgroundImage = (Image)resources.GetObject("PvEButton.BackgroundImage");
            PvEButton.BackgroundImageLayout = ImageLayout.Stretch;
            PvEButton.FlatAppearance.BorderSize = 0;
            PvEButton.FlatStyle = FlatStyle.Flat;
            PvEButton.Font = new Font("Microsoft Sans Serif", 22.2F, FontStyle.Bold, GraphicsUnit.Point, 0);
            PvEButton.Location = new Point(574, 317);
            PvEButton.Name = "PvEButton";
            PvEButton.Size = new Size(254, 78);
            PvEButton.TabIndex = 14;
            PvEButton.Text = "🤖 PvE 🤖 ";
            PvEButton.UseVisualStyleBackColor = true;
            PvEButton.Click += PvEButton_Click;
            // 
            // label1
            // 
            label1.AutoSize = true;
            label1.Font = new Font("Microsoft Sans Serif", 48F, FontStyle.Bold, GraphicsUnit.Point, 0);
            label1.Location = new Point(217, 9);
            label1.Name = "label1";
            label1.Size = new Size(445, 91);
            label1.TabIndex = 15;
            label1.Text = "HomePage";
            // 
            // label2
            // 
            label2.AutoSize = true;
            label2.Font = new Font("Microsoft Sans Serif", 22.2F, FontStyle.Bold, GraphicsUnit.Point, 0);
            label2.Location = new Point(541, 137);
            label2.Name = "label2";
            label2.Size = new Size(287, 126);
            label2.TabIndex = 16;
            label2.Text = "Welcome user,\r\nplease choose \r\nyour play mode";
            // 
            // HomeForm
            // 
            AutoScaleDimensions = new SizeF(8F, 20F);
            AutoScaleMode = AutoScaleMode.Font;
            ClientSize = new Size(876, 607);
            Controls.Add(label2);
            Controls.Add(label1);
            Controls.Add(PvEButton);
            Controls.Add(PvPButton);
            Controls.Add(pictureBox1);
            Name = "HomeForm";
            Text = "HomeForm";
            ((System.ComponentModel.ISupportInitialize)pictureBox1).EndInit();
            ResumeLayout(false);
            PerformLayout();
        }

        #endregion

        private PictureBox pictureBox1;
        private Button PvPButton;
        private Button PvEButton;
        private Label label1;
        private Label label2;
    }
}
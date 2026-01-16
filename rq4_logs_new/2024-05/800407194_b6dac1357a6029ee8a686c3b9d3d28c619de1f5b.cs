namespace winform_chat.DashboardForm
{
    partial class PveModeForm
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        //protected override void Dispose(bool disposing)
        //{
        //    if (disposing && (components != null))
        //    {
        //        components.Dispose();
        //    }
        //    base.Dispose(disposing);
        //}

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(PveModeForm));
            EvilButton = new Button();
            label1 = new Label();
            label2 = new Label();
            EasyButton = new Button();
            BabyButton = new Button();
            IntermidiateButton = new Button();
            HardButton = new Button();
            SuspendLayout();
            // 
            // EvilButton
            // 
            EvilButton.BackColor = Color.DarkOrchid;
            EvilButton.BackgroundImage = (Image)resources.GetObject("EvilButton.BackgroundImage");
            EvilButton.BackgroundImageLayout = ImageLayout.Stretch;
            EvilButton.FlatAppearance.BorderSize = 0;
            EvilButton.FlatStyle = FlatStyle.Flat;
            EvilButton.Font = new Font("Microsoft Sans Serif", 22.2F, FontStyle.Bold, GraphicsUnit.Point, 0);
            EvilButton.Location = new Point(110, 324);
            EvilButton.Margin = new Padding(3, 2, 3, 2);
            EvilButton.Name = "EvilButton";
            EvilButton.Size = new Size(249, 58);
            EvilButton.TabIndex = 14;
            EvilButton.Text = "😈 Evil";
            EvilButton.UseVisualStyleBackColor = false;
            EvilButton.Click += EvilButton_Click;
            // 
            // label1
            // 
            label1.AutoSize = true;
            label1.Font = new Font("Microsoft Sans Serif", 48F, FontStyle.Bold, GraphicsUnit.Point, 0);
            label1.Location = new Point(190, 7);
            label1.Name = "label1";
            label1.Size = new Size(0, 73);
            label1.TabIndex = 15;
            // 
            // label2
            // 
            label2.AutoSize = true;
            label2.Font = new Font("Microsoft Sans Serif", 22.2F, FontStyle.Bold, GraphicsUnit.Point, 0);
            label2.Location = new Point(420, 138);
            label2.Name = "label2";
            label2.Size = new Size(270, 144);
            label2.TabIndex = 16;
            label2.Text = "You have choose \r\n      PVE mode \r\nPlease choose \r\nyour play mode";
            label2.Click += label2_Click;
            // 
            // EasyButton
            // 
            EasyButton.BackColor = Color.FromArgb(128, 255, 255);
            EasyButton.BackgroundImage = (Image)resources.GetObject("EasyButton.BackgroundImage");
            EasyButton.BackgroundImageLayout = ImageLayout.Stretch;
            EasyButton.FlatAppearance.BorderSize = 0;
            EasyButton.FlatStyle = FlatStyle.Flat;
            EasyButton.Font = new Font("Microsoft Sans Serif", 22.2F, FontStyle.Bold, GraphicsUnit.Point, 0);
            EasyButton.Location = new Point(110, 138);
            EasyButton.Margin = new Padding(3, 2, 3, 2);
            EasyButton.Name = "EasyButton";
            EasyButton.Size = new Size(249, 58);
            EasyButton.TabIndex = 17;
            EasyButton.Text = "Easy";
            EasyButton.UseVisualStyleBackColor = false;
            EasyButton.Click += EasyButton_Click;
            // 
            // BabyButton
            // 
            BabyButton.BackColor = Color.Lime;
            BabyButton.BackgroundImage = (Image)resources.GetObject("BabyButton.BackgroundImage");
            BabyButton.BackgroundImageLayout = ImageLayout.Stretch;
            BabyButton.FlatAppearance.BorderSize = 0;
            BabyButton.FlatStyle = FlatStyle.Flat;
            BabyButton.Font = new Font("Microsoft Sans Serif", 22.2F, FontStyle.Bold, GraphicsUnit.Point, 0);
            BabyButton.Location = new Point(110, 76);
            BabyButton.Margin = new Padding(3, 2, 3, 2);
            BabyButton.Name = "BabyButton";
            BabyButton.Size = new Size(249, 58);
            BabyButton.TabIndex = 18;
            BabyButton.Text = "👶 Baby";
            BabyButton.UseVisualStyleBackColor = false;
            BabyButton.Click += BabyButton_Click;
            // 
            // IntermidiateButton
            // 
            IntermidiateButton.BackColor = Color.Yellow;
            IntermidiateButton.BackgroundImage = (Image)resources.GetObject("IntermidiateButton.BackgroundImage");
            IntermidiateButton.BackgroundImageLayout = ImageLayout.Stretch;
            IntermidiateButton.FlatAppearance.BorderSize = 0;
            IntermidiateButton.FlatStyle = FlatStyle.Flat;
            IntermidiateButton.Font = new Font("Microsoft Sans Serif", 22.2F, FontStyle.Bold, GraphicsUnit.Point, 0);
            IntermidiateButton.Location = new Point(110, 200);
            IntermidiateButton.Margin = new Padding(3, 2, 3, 2);
            IntermidiateButton.Name = "IntermidiateButton";
            IntermidiateButton.Size = new Size(249, 58);
            IntermidiateButton.TabIndex = 19;
            IntermidiateButton.Text = "Intermidiate";
            IntermidiateButton.UseVisualStyleBackColor = false;
            IntermidiateButton.Click += IntermidiateButton_Click;
            // 
            // HardButton
            // 
            HardButton.BackColor = Color.LightCoral;
            HardButton.BackgroundImage = (Image)resources.GetObject("HardButton.BackgroundImage");
            HardButton.BackgroundImageLayout = ImageLayout.Stretch;
            HardButton.FlatAppearance.BorderSize = 0;
            HardButton.FlatStyle = FlatStyle.Flat;
            HardButton.Font = new Font("Microsoft Sans Serif", 22.2F, FontStyle.Bold, GraphicsUnit.Point, 0);
            HardButton.Location = new Point(110, 262);
            HardButton.Margin = new Padding(3, 2, 3, 2);
            HardButton.Name = "HardButton";
            HardButton.Size = new Size(249, 58);
            HardButton.TabIndex = 20;
            HardButton.Text = "Hard";
            HardButton.UseVisualStyleBackColor = false;
            HardButton.Click += HardButton_Click;
            // 
            // PveModeForm
            // 
            AutoScaleDimensions = new SizeF(7F, 15F);
            AutoScaleMode = AutoScaleMode.Font;
            ClientSize = new Size(766, 455);
            Controls.Add(HardButton);
            Controls.Add(IntermidiateButton);
            Controls.Add(BabyButton);
            Controls.Add(EasyButton);
            Controls.Add(label2);
            Controls.Add(label1);
            Controls.Add(EvilButton);
            Margin = new Padding(3, 2, 3, 2);
            Name = "PveModeForm";
            Text = "PveModeForm";
            ResumeLayout(false);
            PerformLayout();
        }

        #endregion
        private Button EvilButton;
        private Label label1;
        private Label label2;
        private Button EasyButton;
        private Button BabyButton;
        private Button IntermidiateButton;
        private Button HardButton;
    }
}
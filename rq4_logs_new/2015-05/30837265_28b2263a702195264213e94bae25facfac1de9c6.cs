namespace GoF.CasinoCraps.Game
{
    partial class MainForm
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(MainForm));
            this.crapsLayoutPictureBox = new System.Windows.Forms.PictureBox();
            this.activeBetsGroup = new System.Windows.Forms.GroupBox();
            this.activeBetsListBox = new System.Windows.Forms.ListBox();
            this.completedBetsGroup = new System.Windows.Forms.GroupBox();
            this.completedBetsListBox = new System.Windows.Forms.ListBox();
            this.placeBetButton = new System.Windows.Forms.Button();
            this.betAmountTextBox = new System.Windows.Forms.TextBox();
            this.betAmountLabel = new System.Windows.Forms.Label();
            this.comboBox1 = new System.Windows.Forms.ComboBox();
            this.betLabel = new System.Windows.Forms.Label();
            this.bettingGroup = new System.Windows.Forms.GroupBox();
            this.rollingGroup = new System.Windows.Forms.GroupBox();
            this.firstDiePictureBox = new System.Windows.Forms.PictureBox();
            this.secondDiePictureBox = new System.Windows.Forms.PictureBox();
            this.rollDiceButton = new System.Windows.Forms.Button();
            this.currentMoneyLabel = new System.Windows.Forms.Label();
            ((System.ComponentModel.ISupportInitialize)(this.crapsLayoutPictureBox)).BeginInit();
            this.activeBetsGroup.SuspendLayout();
            this.completedBetsGroup.SuspendLayout();
            this.bettingGroup.SuspendLayout();
            this.rollingGroup.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.firstDiePictureBox)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.secondDiePictureBox)).BeginInit();
            this.SuspendLayout();
            // 
            // crapsLayoutPictureBox
            // 
            this.crapsLayoutPictureBox.Dock = System.Windows.Forms.DockStyle.Top;
            this.crapsLayoutPictureBox.Image = global::GoF.CasinoCraps.Game.Properties.Resources.CrapsTable;
            this.crapsLayoutPictureBox.Location = new System.Drawing.Point(0, 0);
            this.crapsLayoutPictureBox.Name = "crapsLayoutPictureBox";
            this.crapsLayoutPictureBox.Size = new System.Drawing.Size(799, 399);
            this.crapsLayoutPictureBox.TabIndex = 0;
            this.crapsLayoutPictureBox.TabStop = false;
            // 
            // activeBetsGroup
            // 
            this.activeBetsGroup.Controls.Add(this.activeBetsListBox);
            this.activeBetsGroup.Location = new System.Drawing.Point(12, 405);
            this.activeBetsGroup.Name = "activeBetsGroup";
            this.activeBetsGroup.Size = new System.Drawing.Size(181, 292);
            this.activeBetsGroup.TabIndex = 1;
            this.activeBetsGroup.TabStop = false;
            this.activeBetsGroup.Text = "Active Bets";
            // 
            // activeBetsListBox
            // 
            this.activeBetsListBox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.activeBetsListBox.FormattingEnabled = true;
            this.activeBetsListBox.Location = new System.Drawing.Point(3, 16);
            this.activeBetsListBox.Name = "activeBetsListBox";
            this.activeBetsListBox.Size = new System.Drawing.Size(175, 273);
            this.activeBetsListBox.TabIndex = 0;
            // 
            // completedBetsGroup
            // 
            this.completedBetsGroup.Controls.Add(this.completedBetsListBox);
            this.completedBetsGroup.Location = new System.Drawing.Point(199, 405);
            this.completedBetsGroup.Name = "completedBetsGroup";
            this.completedBetsGroup.Size = new System.Drawing.Size(181, 292);
            this.completedBetsGroup.TabIndex = 2;
            this.completedBetsGroup.TabStop = false;
            this.completedBetsGroup.Text = "Completed Bets";
            // 
            // completedBetsListBox
            // 
            this.completedBetsListBox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.completedBetsListBox.FormattingEnabled = true;
            this.completedBetsListBox.Location = new System.Drawing.Point(3, 16);
            this.completedBetsListBox.Name = "completedBetsListBox";
            this.completedBetsListBox.Size = new System.Drawing.Size(175, 273);
            this.completedBetsListBox.TabIndex = 0;
            // 
            // placeBetButton
            // 
            this.placeBetButton.Location = new System.Drawing.Point(199, 67);
            this.placeBetButton.Name = "placeBetButton";
            this.placeBetButton.Size = new System.Drawing.Size(75, 23);
            this.placeBetButton.TabIndex = 3;
            this.placeBetButton.Text = "Place Bet";
            this.placeBetButton.UseVisualStyleBackColor = true;
            // 
            // betAmountTextBox
            // 
            this.betAmountTextBox.Location = new System.Drawing.Point(169, 41);
            this.betAmountTextBox.Name = "betAmountTextBox";
            this.betAmountTextBox.Size = new System.Drawing.Size(105, 20);
            this.betAmountTextBox.TabIndex = 4;
            // 
            // betAmountLabel
            // 
            this.betAmountLabel.AutoSize = true;
            this.betAmountLabel.Location = new System.Drawing.Point(84, 44);
            this.betAmountLabel.Name = "betAmountLabel";
            this.betAmountLabel.Size = new System.Drawing.Size(80, 13);
            this.betAmountLabel.TabIndex = 5;
            this.betAmountLabel.Text = "Bet Amount ($):";
            // 
            // comboBox1
            // 
            this.comboBox1.FormattingEnabled = true;
            this.comboBox1.Location = new System.Drawing.Point(169, 14);
            this.comboBox1.Name = "comboBox1";
            this.comboBox1.Size = new System.Drawing.Size(105, 21);
            this.comboBox1.TabIndex = 6;
            // 
            // betLabel
            // 
            this.betLabel.AutoSize = true;
            this.betLabel.Location = new System.Drawing.Point(110, 17);
            this.betLabel.Name = "betLabel";
            this.betLabel.Size = new System.Drawing.Size(53, 13);
            this.betLabel.TabIndex = 7;
            this.betLabel.Text = "Bet Type:";
            // 
            // bettingGroup
            // 
            this.bettingGroup.Controls.Add(this.comboBox1);
            this.bettingGroup.Controls.Add(this.betLabel);
            this.bettingGroup.Controls.Add(this.placeBetButton);
            this.bettingGroup.Controls.Add(this.betAmountTextBox);
            this.bettingGroup.Controls.Add(this.betAmountLabel);
            this.bettingGroup.Location = new System.Drawing.Point(386, 440);
            this.bettingGroup.Name = "bettingGroup";
            this.bettingGroup.Size = new System.Drawing.Size(401, 98);
            this.bettingGroup.TabIndex = 8;
            this.bettingGroup.TabStop = false;
            this.bettingGroup.Text = "Betting";
            // 
            // rollingGroup
            // 
            this.rollingGroup.Controls.Add(this.rollDiceButton);
            this.rollingGroup.Controls.Add(this.secondDiePictureBox);
            this.rollingGroup.Controls.Add(this.firstDiePictureBox);
            this.rollingGroup.Location = new System.Drawing.Point(386, 544);
            this.rollingGroup.Name = "rollingGroup";
            this.rollingGroup.Size = new System.Drawing.Size(401, 153);
            this.rollingGroup.TabIndex = 9;
            this.rollingGroup.TabStop = false;
            this.rollingGroup.Text = "Rolling";
            // 
            // firstDiePictureBox
            // 
            this.firstDiePictureBox.Location = new System.Drawing.Point(6, 20);
            this.firstDiePictureBox.Name = "firstDiePictureBox";
            this.firstDiePictureBox.Size = new System.Drawing.Size(125, 125);
            this.firstDiePictureBox.TabIndex = 0;
            this.firstDiePictureBox.TabStop = false;
            // 
            // secondDiePictureBox
            // 
            this.secondDiePictureBox.Location = new System.Drawing.Point(137, 20);
            this.secondDiePictureBox.Name = "secondDiePictureBox";
            this.secondDiePictureBox.Size = new System.Drawing.Size(125, 125);
            this.secondDiePictureBox.TabIndex = 1;
            this.secondDiePictureBox.TabStop = false;
            // 
            // rollDiceButton
            // 
            this.rollDiceButton.Location = new System.Drawing.Point(294, 65);
            this.rollDiceButton.Name = "rollDiceButton";
            this.rollDiceButton.Size = new System.Drawing.Size(75, 23);
            this.rollDiceButton.TabIndex = 2;
            this.rollDiceButton.Text = "Roll Dice";
            this.rollDiceButton.UseVisualStyleBackColor = true;
            // 
            // currentMoneyLabel
            // 
            this.currentMoneyLabel.AutoSize = true;
            this.currentMoneyLabel.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.currentMoneyLabel.Location = new System.Drawing.Point(503, 414);
            this.currentMoneyLabel.Name = "currentMoneyLabel";
            this.currentMoneyLabel.Size = new System.Drawing.Size(186, 20);
            this.currentMoneyLabel.TabIndex = 10;
            this.currentMoneyLabel.Text = "Current Money: $5000";
            // 
            // MainForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(799, 709);
            this.Controls.Add(this.currentMoneyLabel);
            this.Controls.Add(this.rollingGroup);
            this.Controls.Add(this.bettingGroup);
            this.Controls.Add(this.completedBetsGroup);
            this.Controls.Add(this.activeBetsGroup);
            this.Controls.Add(this.crapsLayoutPictureBox);
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.MaximizeBox = false;
            this.Name = "MainForm";
            this.Text = "Casino Craps";
            ((System.ComponentModel.ISupportInitialize)(this.crapsLayoutPictureBox)).EndInit();
            this.activeBetsGroup.ResumeLayout(false);
            this.completedBetsGroup.ResumeLayout(false);
            this.bettingGroup.ResumeLayout(false);
            this.bettingGroup.PerformLayout();
            this.rollingGroup.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.firstDiePictureBox)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.secondDiePictureBox)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.PictureBox crapsLayoutPictureBox;
        private System.Windows.Forms.GroupBox activeBetsGroup;
        private System.Windows.Forms.ListBox activeBetsListBox;
        private System.Windows.Forms.GroupBox completedBetsGroup;
        private System.Windows.Forms.ListBox completedBetsListBox;
        private System.Windows.Forms.Button placeBetButton;
        private System.Windows.Forms.TextBox betAmountTextBox;
        private System.Windows.Forms.Label betAmountLabel;
        private System.Windows.Forms.ComboBox comboBox1;
        private System.Windows.Forms.Label betLabel;
        private System.Windows.Forms.GroupBox bettingGroup;
        private System.Windows.Forms.GroupBox rollingGroup;
        private System.Windows.Forms.Button rollDiceButton;
        private System.Windows.Forms.PictureBox secondDiePictureBox;
        private System.Windows.Forms.PictureBox firstDiePictureBox;
        private System.Windows.Forms.Label currentMoneyLabel;
    }
}

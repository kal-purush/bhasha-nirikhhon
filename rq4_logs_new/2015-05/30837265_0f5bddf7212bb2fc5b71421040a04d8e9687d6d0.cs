namespace GoF.CasinoCraps.UserInterface
{
    partial class SetRollForm
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
            this.okButton = new System.Windows.Forms.Button();
            this.cancelButton = new System.Windows.Forms.Button();
            this.firstDieLabel = new System.Windows.Forms.Label();
            this.secondDieLabel = new System.Windows.Forms.Label();
            this.firstDieUpDown = new System.Windows.Forms.NumericUpDown();
            this.secondDieUpDown = new System.Windows.Forms.NumericUpDown();
            ((System.ComponentModel.ISupportInitialize)(this.firstDieUpDown)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.secondDieUpDown)).BeginInit();
            this.SuspendLayout();
            // 
            // okButton
            // 
            this.okButton.Location = new System.Drawing.Point(11, 59);
            this.okButton.Name = "okButton";
            this.okButton.Size = new System.Drawing.Size(75, 23);
            this.okButton.TabIndex = 0;
            this.okButton.Text = "OK";
            this.okButton.UseVisualStyleBackColor = true;
            this.okButton.Click += new System.EventHandler(this.okButton_Click);
            // 
            // cancelButton
            // 
            this.cancelButton.DialogResult = System.Windows.Forms.DialogResult.Cancel;
            this.cancelButton.Location = new System.Drawing.Point(92, 59);
            this.cancelButton.Name = "cancelButton";
            this.cancelButton.Size = new System.Drawing.Size(75, 23);
            this.cancelButton.TabIndex = 1;
            this.cancelButton.Text = "Cancel";
            this.cancelButton.UseVisualStyleBackColor = true;
            this.cancelButton.Click += new System.EventHandler(this.cancelButton_Click);
            // 
            // firstDieLabel
            // 
            this.firstDieLabel.AutoSize = true;
            this.firstDieLabel.Location = new System.Drawing.Point(46, 10);
            this.firstDieLabel.Name = "firstDieLabel";
            this.firstDieLabel.Size = new System.Drawing.Size(48, 13);
            this.firstDieLabel.TabIndex = 2;
            this.firstDieLabel.Text = "First Die:";
            // 
            // secondDieLabel
            // 
            this.secondDieLabel.AutoSize = true;
            this.secondDieLabel.Location = new System.Drawing.Point(28, 36);
            this.secondDieLabel.Name = "secondDieLabel";
            this.secondDieLabel.Size = new System.Drawing.Size(66, 13);
            this.secondDieLabel.TabIndex = 3;
            this.secondDieLabel.Text = "Second Die:";
            // 
            // firstDieUpDown
            // 
            this.firstDieUpDown.Location = new System.Drawing.Point(100, 8);
            this.firstDieUpDown.Maximum = new decimal(new int[] {
            6,
            0,
            0,
            0});
            this.firstDieUpDown.Minimum = new decimal(new int[] {
            1,
            0,
            0,
            0});
            this.firstDieUpDown.Name = "firstDieUpDown";
            this.firstDieUpDown.Size = new System.Drawing.Size(67, 20);
            this.firstDieUpDown.TabIndex = 4;
            this.firstDieUpDown.Value = new decimal(new int[] {
            1,
            0,
            0,
            0});
            // 
            // secondDieUpDown
            // 
            this.secondDieUpDown.Location = new System.Drawing.Point(100, 34);
            this.secondDieUpDown.Maximum = new decimal(new int[] {
            6,
            0,
            0,
            0});
            this.secondDieUpDown.Minimum = new decimal(new int[] {
            1,
            0,
            0,
            0});
            this.secondDieUpDown.Name = "secondDieUpDown";
            this.secondDieUpDown.Size = new System.Drawing.Size(67, 20);
            this.secondDieUpDown.TabIndex = 5;
            this.secondDieUpDown.Value = new decimal(new int[] {
            1,
            0,
            0,
            0});
            // 
            // SetRollForm
            // 
            this.AcceptButton = this.okButton;
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.CancelButton = this.cancelButton;
            this.ClientSize = new System.Drawing.Size(179, 88);
            this.Controls.Add(this.secondDieUpDown);
            this.Controls.Add(this.firstDieUpDown);
            this.Controls.Add(this.secondDieLabel);
            this.Controls.Add(this.firstDieLabel);
            this.Controls.Add(this.cancelButton);
            this.Controls.Add(this.okButton);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedDialog;
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "SetRollForm";
            this.Text = "Set Roll";
            ((System.ComponentModel.ISupportInitialize)(this.firstDieUpDown)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.secondDieUpDown)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button okButton;
        private System.Windows.Forms.Button cancelButton;
        private System.Windows.Forms.Label firstDieLabel;
        private System.Windows.Forms.Label secondDieLabel;
        private System.Windows.Forms.NumericUpDown firstDieUpDown;
        private System.Windows.Forms.NumericUpDown secondDieUpDown;
    }
}
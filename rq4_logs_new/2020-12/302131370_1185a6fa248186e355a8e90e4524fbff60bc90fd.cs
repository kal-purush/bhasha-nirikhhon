namespace Graphics_Editor
{
    partial class Actions
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
            this.drawButton = new System.Windows.Forms.Button();
            this.selectButton = new System.Windows.Forms.Button();
            this.moveButton = new System.Windows.Forms.Button();
            this.vectorX = new System.Windows.Forms.NumericUpDown();
            this.vectorY = new System.Windows.Forms.NumericUpDown();
            this.vectorXLabel = new System.Windows.Forms.Label();
            this.vectorYLabel = new System.Windows.Forms.Label();
            ((System.ComponentModel.ISupportInitialize)(this.vectorX)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.vectorY)).BeginInit();
            this.SuspendLayout();
            // 
            // drawButton
            // 
            this.drawButton.Font = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(238)));
            this.drawButton.Location = new System.Drawing.Point(33, 21);
            this.drawButton.Name = "drawButton";
            this.drawButton.Size = new System.Drawing.Size(250, 30);
            this.drawButton.TabIndex = 39;
            this.drawButton.Text = "Draw";
            this.drawButton.UseVisualStyleBackColor = true;
            this.drawButton.Click += new System.EventHandler(this.drawButton_Click);
            // 
            // selectButton
            // 
            this.selectButton.Font = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(238)));
            this.selectButton.Location = new System.Drawing.Point(33, 57);
            this.selectButton.Name = "selectButton";
            this.selectButton.Size = new System.Drawing.Size(250, 30);
            this.selectButton.TabIndex = 40;
            this.selectButton.Text = "Select";
            this.selectButton.UseVisualStyleBackColor = true;
            this.selectButton.Click += new System.EventHandler(this.selectButton_Click);
            // 
            // moveButton
            // 
            this.moveButton.Font = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(238)));
            this.moveButton.Location = new System.Drawing.Point(33, 93);
            this.moveButton.Name = "moveButton";
            this.moveButton.Size = new System.Drawing.Size(250, 30);
            this.moveButton.TabIndex = 41;
            this.moveButton.Text = "Move by vector...";
            this.moveButton.UseVisualStyleBackColor = true;
            this.moveButton.Click += new System.EventHandler(this.moveButton_Click);
            // 
            // vectorX
            // 
            this.vectorX.Font = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(238)));
            this.vectorX.Location = new System.Drawing.Point(333, 93);
            this.vectorX.Maximum = new decimal(new int[] {
            1000,
            0,
            0,
            0});
            this.vectorX.Minimum = new decimal(new int[] {
            1000,
            0,
            0,
            -2147483648});
            this.vectorX.Name = "vectorX";
            this.vectorX.Size = new System.Drawing.Size(51, 29);
            this.vectorX.TabIndex = 43;
            // 
            // vectorY
            // 
            this.vectorY.Font = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(238)));
            this.vectorY.Location = new System.Drawing.Point(430, 93);
            this.vectorY.Maximum = new decimal(new int[] {
            1000,
            0,
            0,
            0});
            this.vectorY.Minimum = new decimal(new int[] {
            1000,
            0,
            0,
            -2147483648});
            this.vectorY.Name = "vectorY";
            this.vectorY.Size = new System.Drawing.Size(51, 29);
            this.vectorY.TabIndex = 44;
            // 
            // vectorXLabel
            // 
            this.vectorXLabel.AutoSize = true;
            this.vectorXLabel.Font = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(238)));
            this.vectorXLabel.Location = new System.Drawing.Point(310, 95);
            this.vectorXLabel.Name = "vectorXLabel";
            this.vectorXLabel.Size = new System.Drawing.Size(17, 21);
            this.vectorXLabel.TabIndex = 45;
            this.vectorXLabel.Text = "x";
            // 
            // vectorYLabel
            // 
            this.vectorYLabel.AutoSize = true;
            this.vectorYLabel.Font = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(238)));
            this.vectorYLabel.Location = new System.Drawing.Point(407, 95);
            this.vectorYLabel.Name = "vectorYLabel";
            this.vectorYLabel.Size = new System.Drawing.Size(18, 21);
            this.vectorYLabel.TabIndex = 46;
            this.vectorYLabel.Text = "y";
            // 
            // Actions
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(491, 143);
            this.Controls.Add(this.vectorYLabel);
            this.Controls.Add(this.vectorXLabel);
            this.Controls.Add(this.vectorY);
            this.Controls.Add(this.vectorX);
            this.Controls.Add(this.moveButton);
            this.Controls.Add(this.selectButton);
            this.Controls.Add(this.drawButton);
            this.Name = "Actions";
            this.Text = "Polygon Actions";
            ((System.ComponentModel.ISupportInitialize)(this.vectorX)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.vectorY)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button drawButton;
        private System.Windows.Forms.Button selectButton;
        private System.Windows.Forms.Button moveButton;
        private System.Windows.Forms.NumericUpDown vectorX;
        private System.Windows.Forms.NumericUpDown vectorY;
        private System.Windows.Forms.Label vectorXLabel;
        private System.Windows.Forms.Label vectorYLabel;
    }
}
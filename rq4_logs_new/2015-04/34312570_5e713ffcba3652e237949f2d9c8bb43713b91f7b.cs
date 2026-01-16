namespace SquareRootsSolver
{
    partial class NewGridDialog
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
            this.itsButtonOkay = new System.Windows.Forms.Button();
            this.itsNumericGridSize = new System.Windows.Forms.NumericUpDown();
            this.itsLabelGridSize = new System.Windows.Forms.Label();
            ((System.ComponentModel.ISupportInitialize)(this.itsNumericGridSize)).BeginInit();
            this.SuspendLayout();
            // 
            // itsButtonOkay
            // 
            this.itsButtonOkay.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.itsButtonOkay.Location = new System.Drawing.Point(58, 70);
            this.itsButtonOkay.Name = "itsButtonOkay";
            this.itsButtonOkay.Size = new System.Drawing.Size(75, 23);
            this.itsButtonOkay.TabIndex = 0;
            this.itsButtonOkay.Text = "Okay";
            this.itsButtonOkay.UseVisualStyleBackColor = true;
            this.itsButtonOkay.Click += new System.EventHandler(this.ItsButtonOkay_Click);
            // 
            // itsNumericGridSize
            // 
            this.itsNumericGridSize.Location = new System.Drawing.Point(141, 30);
            this.itsNumericGridSize.Name = "itsNumericGridSize";
            this.itsNumericGridSize.Size = new System.Drawing.Size(37, 20);
            this.itsNumericGridSize.TabIndex = 1;
            // 
            // itsLabelGridSize
            // 
            this.itsLabelGridSize.AutoSize = true;
            this.itsLabelGridSize.Location = new System.Drawing.Point(27, 32);
            this.itsLabelGridSize.Name = "itsLabelGridSize";
            this.itsLabelGridSize.Size = new System.Drawing.Size(108, 13);
            this.itsLabelGridSize.TabIndex = 2;
            this.itsLabelGridSize.Text = "What size is the grid?";
            // 
            // NewGridDialog
            // 
            this.AcceptButton = this.itsButtonOkay;
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(190, 105);
            this.Controls.Add(this.itsLabelGridSize);
            this.Controls.Add(this.itsNumericGridSize);
            this.Controls.Add(this.itsButtonOkay);
            this.ForeColor = System.Drawing.SystemColors.ControlText;
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedDialog;
            this.Name = "NewGridDialog";
            this.Text = "New Grid";
            ((System.ComponentModel.ISupportInitialize)(this.itsNumericGridSize)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button itsButtonOkay;
        private System.Windows.Forms.NumericUpDown itsNumericGridSize;
        private System.Windows.Forms.Label itsLabelGridSize;
    }
}
namespace EllieLogicShared
{
    partial class EllieAjuda
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

        #region Component Designer generated code

        /// <summary> 
        /// Required method for Designer support - do not modify 
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.picHelp = new System.Windows.Forms.PictureBox();
            this.picEllie = new System.Windows.Forms.PictureBox();
            ((System.ComponentModel.ISupportInitialize)(this.picHelp)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.picEllie)).BeginInit();
            this.SuspendLayout();
            // 
            // picHelp
            // 
            this.picHelp.BackColor = System.Drawing.Color.Transparent;
            this.picHelp.Image = global::EllieLogicShared.Properties.Resources.helpGif1;
            this.picHelp.Location = new System.Drawing.Point(102, -2);
            this.picHelp.Name = "picHelp";
            this.picHelp.Size = new System.Drawing.Size(40, 50);
            this.picHelp.SizeMode = System.Windows.Forms.PictureBoxSizeMode.Zoom;
            this.picHelp.TabIndex = 10;
            this.picHelp.TabStop = false;
            // 
            // picEllie
            // 
            this.picEllie.BackColor = System.Drawing.Color.Transparent;
            this.picEllie.Image = global::EllieLogicShared.Properties.Resources.mascoteGif1;
            this.picEllie.Location = new System.Drawing.Point(0, 0);
            this.picEllie.Name = "picEllie";
            this.picEllie.Size = new System.Drawing.Size(124, 126);
            this.picEllie.SizeMode = System.Windows.Forms.PictureBoxSizeMode.CenterImage;
            this.picEllie.TabIndex = 9;
            this.picEllie.TabStop = false;
            // 
            // EllieAjuda
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.Color.Transparent;
            this.Controls.Add(this.picHelp);
            this.Controls.Add(this.picEllie);
            this.Name = "EllieAjuda";
            this.Size = new System.Drawing.Size(146, 129);
            ((System.ComponentModel.ISupportInitialize)(this.picHelp)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.picEllie)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.PictureBox picHelp;
        private System.Windows.Forms.PictureBox picEllie;
    }
}
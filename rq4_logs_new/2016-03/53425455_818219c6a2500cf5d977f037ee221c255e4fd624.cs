namespace DATAO
{
    partial class StartForm
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
            this.pictureBox1 = new System.Windows.Forms.PictureBox();
            this.gAuthFormTextField = new MaterialSkin.Controls.MaterialSingleLineTextField();
            this.gAuthFormLabel = new MaterialSkin.Controls.MaterialLabel();
            this.gAuthConfirmButton = new MaterialSkin.Controls.MaterialFlatButton();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox1)).BeginInit();
            this.SuspendLayout();
            // 
            // pictureBox1
            // 
            this.pictureBox1.Image = global::DATAO.Properties.Resources.bigstock_Abstract_White_Digital_D_Chao_75094717;
            this.pictureBox1.Location = new System.Drawing.Point(0, 64);
            this.pictureBox1.Name = "pictureBox1";
            this.pictureBox1.Size = new System.Drawing.Size(710, 276);
            this.pictureBox1.SizeMode = System.Windows.Forms.PictureBoxSizeMode.CenterImage;
            this.pictureBox1.TabIndex = 0;
            this.pictureBox1.TabStop = false;
            // 
            // gAuthFormTextField
            // 
            this.gAuthFormTextField.Depth = 0;
            this.gAuthFormTextField.Hint = "";
            this.gAuthFormTextField.Location = new System.Drawing.Point(196, 204);
            this.gAuthFormTextField.MouseState = MaterialSkin.MouseState.HOVER;
            this.gAuthFormTextField.Name = "gAuthFormTextField";
            this.gAuthFormTextField.PasswordChar = '\0';
            this.gAuthFormTextField.SelectedText = "";
            this.gAuthFormTextField.SelectionLength = 0;
            this.gAuthFormTextField.SelectionStart = 0;
            this.gAuthFormTextField.Size = new System.Drawing.Size(304, 23);
            this.gAuthFormTextField.TabIndex = 1;
            this.gAuthFormTextField.UseSystemPasswordChar = false;
            // 
            // gAuthFormLabel
            // 
            this.gAuthFormLabel.AutoSize = true;
            this.gAuthFormLabel.Depth = 0;
            this.gAuthFormLabel.Font = new System.Drawing.Font("Roboto", 11F);
            this.gAuthFormLabel.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(222)))), ((int)(((byte)(0)))), ((int)(((byte)(0)))), ((int)(((byte)(0)))));
            this.gAuthFormLabel.Location = new System.Drawing.Point(192, 182);
            this.gAuthFormLabel.MouseState = MaterialSkin.MouseState.HOVER;
            this.gAuthFormLabel.Name = "gAuthFormLabel";
            this.gAuthFormLabel.Size = new System.Drawing.Size(143, 19);
            this.gAuthFormLabel.TabIndex = 2;
            this.gAuthFormLabel.Text = "Введите код сюда:";
            // 
            // gAuthConfirmButton
            // 
            this.gAuthConfirmButton.AutoSize = true;
            this.gAuthConfirmButton.AutoSizeMode = System.Windows.Forms.AutoSizeMode.GrowAndShrink;
            this.gAuthConfirmButton.Depth = 0;
            this.gAuthConfirmButton.Location = new System.Drawing.Point(283, 236);
            this.gAuthConfirmButton.Margin = new System.Windows.Forms.Padding(4, 6, 4, 6);
            this.gAuthConfirmButton.MouseState = MaterialSkin.MouseState.HOVER;
            this.gAuthConfirmButton.Name = "gAuthConfirmButton";
            this.gAuthConfirmButton.Primary = false;
            this.gAuthConfirmButton.Size = new System.Drawing.Size(112, 36);
            this.gAuthConfirmButton.TabIndex = 3;
            this.gAuthConfirmButton.Text = "ПОДТВЕРДИТЬ";
            this.gAuthConfirmButton.UseVisualStyleBackColor = true;
            this.gAuthConfirmButton.Click += new System.EventHandler(this.materialFlatButton1_Click);
            // 
            // StartForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(710, 340);
            this.Controls.Add(this.gAuthConfirmButton);
            this.Controls.Add(this.gAuthFormLabel);
            this.Controls.Add(this.gAuthFormTextField);
            this.Controls.Add(this.pictureBox1);
            this.Name = "StartForm";
            this.Text = "Google OAUTH 2.0 Verification";
            this.Load += new System.EventHandler(this.StartForm_Load);
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox1)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.PictureBox pictureBox1;
        private MaterialSkin.Controls.MaterialSingleLineTextField gAuthFormTextField;
        private MaterialSkin.Controls.MaterialLabel gAuthFormLabel;
        private MaterialSkin.Controls.MaterialFlatButton gAuthConfirmButton;
    }
}
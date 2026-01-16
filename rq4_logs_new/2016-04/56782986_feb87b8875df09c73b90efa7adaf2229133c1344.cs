namespace JarakTerdekat
{
    partial class inputJarakDialogue
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
            this.txtField_jarakInput = new MaterialSkin.Controls.MaterialSingleLineTextField();
            this.btn_updateJarak = new MaterialSkin.Controls.MaterialRaisedButton();
            this.SuspendLayout();
            // 
            // txtField_jarakInput
            // 
            this.txtField_jarakInput.BackColor = System.Drawing.Color.White;
            this.txtField_jarakInput.Depth = 0;
            this.txtField_jarakInput.Hint = "Jarak";
            this.txtField_jarakInput.Location = new System.Drawing.Point(12, 80);
            this.txtField_jarakInput.MaxLength = 32767;
            this.txtField_jarakInput.MouseState = MaterialSkin.MouseState.HOVER;
            this.txtField_jarakInput.Name = "txtField_jarakInput";
            this.txtField_jarakInput.PasswordChar = '\0';
            this.txtField_jarakInput.SelectedText = "";
            this.txtField_jarakInput.SelectionLength = 0;
            this.txtField_jarakInput.SelectionStart = 0;
            this.txtField_jarakInput.Size = new System.Drawing.Size(262, 23);
            this.txtField_jarakInput.TabIndex = 0;
            this.txtField_jarakInput.TabStop = false;
            this.txtField_jarakInput.UseSystemPasswordChar = false;
            // 
            // btn_updateJarak
            // 
            this.btn_updateJarak.Depth = 0;
            this.btn_updateJarak.DialogResult = System.Windows.Forms.DialogResult.OK;
            this.btn_updateJarak.Location = new System.Drawing.Point(64, 136);
            this.btn_updateJarak.MouseState = MaterialSkin.MouseState.HOVER;
            this.btn_updateJarak.Name = "btn_updateJarak";
            this.btn_updateJarak.Primary = true;
            this.btn_updateJarak.Size = new System.Drawing.Size(154, 30);
            this.btn_updateJarak.TabIndex = 1;
            this.btn_updateJarak.Text = "Update";
            this.btn_updateJarak.UseVisualStyleBackColor = true;
            this.btn_updateJarak.Click += new System.EventHandler(this.btn_updateJarak_Click);
            // 
            // inputJarakDialogue
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(286, 199);
            this.Controls.Add(this.btn_updateJarak);
            this.Controls.Add(this.txtField_jarakInput);
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "inputJarakDialogue";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Ubah Jarak";
            this.ResumeLayout(false);

        }

        #endregion

        private MaterialSkin.Controls.MaterialSingleLineTextField txtField_jarakInput;
        private MaterialSkin.Controls.MaterialRaisedButton btn_updateJarak;
    }
}
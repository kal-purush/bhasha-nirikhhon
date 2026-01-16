namespace SIvPaVS_App.CustomMessageBoxes
{
    partial class form_msg_CreateNew
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
            this.lbl_InfoText = new System.Windows.Forms.Label();
            this.lbl_DocType = new System.Windows.Forms.Label();
            this.cb_DocType = new System.Windows.Forms.ComboBox();
            this.btn_Create = new System.Windows.Forms.Button();
            this.btn_Cancel = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // lbl_InfoText
            // 
            this.lbl_InfoText.AutoSize = true;
            this.lbl_InfoText.Font = new System.Drawing.Font("Lucida Sans Unicode", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lbl_InfoText.Location = new System.Drawing.Point(80, 23);
            this.lbl_InfoText.Name = "lbl_InfoText";
            this.lbl_InfoText.Size = new System.Drawing.Size(441, 20);
            this.lbl_InfoText.TabIndex = 0;
            this.lbl_InfoText.Text = "Prosím, zvolte si typ dokuemntu, ktorý chcete vytvoriť.";
            // 
            // lbl_DocType
            // 
            this.lbl_DocType.AutoSize = true;
            this.lbl_DocType.Location = new System.Drawing.Point(81, 101);
            this.lbl_DocType.Name = "lbl_DocType";
            this.lbl_DocType.Size = new System.Drawing.Size(84, 13);
            this.lbl_DocType.TabIndex = 1;
            this.lbl_DocType.Text = "Typ dokumentu:";
            // 
            // cb_DocType
            // 
            this.cb_DocType.FormattingEnabled = true;
            this.cb_DocType.Items.AddRange(new object[] {
            "Formulár"});
            this.cb_DocType.Location = new System.Drawing.Point(171, 98);
            this.cb_DocType.Name = "cb_DocType";
            this.cb_DocType.Size = new System.Drawing.Size(350, 21);
            this.cb_DocType.TabIndex = 2;
            // 
            // btn_Create
            // 
            this.btn_Create.Location = new System.Drawing.Point(200, 215);
            this.btn_Create.Name = "btn_Create";
            this.btn_Create.Size = new System.Drawing.Size(75, 23);
            this.btn_Create.TabIndex = 3;
            this.btn_Create.Text = "Vytvoriť";
            this.btn_Create.UseVisualStyleBackColor = true;
            this.btn_Create.Click += new System.EventHandler(this.eh_btn_Click);
            // 
            // btn_Cancel
            // 
            this.btn_Cancel.Location = new System.Drawing.Point(325, 215);
            this.btn_Cancel.Name = "btn_Cancel";
            this.btn_Cancel.Size = new System.Drawing.Size(75, 23);
            this.btn_Cancel.TabIndex = 4;
            this.btn_Cancel.Text = "Zrušiť";
            this.btn_Cancel.UseVisualStyleBackColor = true;
            this.btn_Cancel.Click += new System.EventHandler(this.eh_btn_Click);
            // 
            // form_msg_CreateNew
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(584, 261);
            this.ControlBox = false;
            this.Controls.Add(this.btn_Cancel);
            this.Controls.Add(this.btn_Create);
            this.Controls.Add(this.cb_DocType);
            this.Controls.Add(this.lbl_DocType);
            this.Controls.Add(this.lbl_InfoText);
            this.Name = "form_msg_CreateNew";
            this.Text = "Create New...";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label lbl_InfoText;
        private System.Windows.Forms.Label lbl_DocType;
        private System.Windows.Forms.ComboBox cb_DocType;
        private System.Windows.Forms.Button btn_Create;
        private System.Windows.Forms.Button btn_Cancel;
    }
}
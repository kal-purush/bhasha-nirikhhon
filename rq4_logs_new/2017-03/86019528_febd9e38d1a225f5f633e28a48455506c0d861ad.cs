namespace ConverterApp
{
    partial class frm_Main
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
            this.lbl_UofM = new System.Windows.Forms.Label();
            this.txt_UnitOfMeasure = new System.Windows.Forms.TextBox();
            this.btn_CM_to_Inches = new System.Windows.Forms.Button();
            this.btn_M_to_Feet = new System.Windows.Forms.Button();
            this.btn_Exit = new System.Windows.Forms.Button();
            this.listBox = new System.Windows.Forms.ListBox();
            this.btn_C_to_F = new System.Windows.Forms.Button();
            this.btn_CM_to_Feet = new System.Windows.Forms.Button();
            this.btn_KM_to_Miles = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // lbl_UofM
            // 
            this.lbl_UofM.AutoSize = true;
            this.lbl_UofM.Location = new System.Drawing.Point(31, 18);
            this.lbl_UofM.Margin = new System.Windows.Forms.Padding(2, 0, 2, 0);
            this.lbl_UofM.Name = "lbl_UofM";
            this.lbl_UofM.Size = new System.Drawing.Size(82, 13);
            this.lbl_UofM.TabIndex = 0;
            this.lbl_UofM.Text = "Unit of Measure";
            // 
            // txt_UnitOfMeasure
            // 
            this.txt_UnitOfMeasure.Location = new System.Drawing.Point(133, 15);
            this.txt_UnitOfMeasure.Margin = new System.Windows.Forms.Padding(2);
            this.txt_UnitOfMeasure.Name = "txt_UnitOfMeasure";
            this.txt_UnitOfMeasure.Size = new System.Drawing.Size(150, 20);
            this.txt_UnitOfMeasure.TabIndex = 1;
            // 
            // btn_CM_to_Inches
            // 
            this.btn_CM_to_Inches.Location = new System.Drawing.Point(133, 51);
            this.btn_CM_to_Inches.Margin = new System.Windows.Forms.Padding(2);
            this.btn_CM_to_Inches.Name = "btn_CM_to_Inches";
            this.btn_CM_to_Inches.Size = new System.Drawing.Size(122, 19);
            this.btn_CM_to_Inches.TabIndex = 2;
            this.btn_CM_to_Inches.Text = "Centimetres to Inches";
            this.btn_CM_to_Inches.UseVisualStyleBackColor = true;
            this.btn_CM_to_Inches.Click += new System.EventHandler(this.btn_CM_to_Inches_Click);
            // 
            // btn_M_to_Feet
            // 
            this.btn_M_to_Feet.Location = new System.Drawing.Point(133, 87);
            this.btn_M_to_Feet.Margin = new System.Windows.Forms.Padding(2);
            this.btn_M_to_Feet.Name = "btn_M_to_Feet";
            this.btn_M_to_Feet.Size = new System.Drawing.Size(122, 19);
            this.btn_M_to_Feet.TabIndex = 3;
            this.btn_M_to_Feet.Text = "Metres to Feet";
            this.btn_M_to_Feet.UseVisualStyleBackColor = true;
            this.btn_M_to_Feet.Click += new System.EventHandler(this.btn_M_to_Feet_Click);
            // 
            // btn_Exit
            // 
            this.btn_Exit.Location = new System.Drawing.Point(144, 443);
            this.btn_Exit.Margin = new System.Windows.Forms.Padding(2);
            this.btn_Exit.Name = "btn_Exit";
            this.btn_Exit.Size = new System.Drawing.Size(86, 22);
            this.btn_Exit.TabIndex = 4;
            this.btn_Exit.Text = "Exit";
            this.btn_Exit.UseVisualStyleBackColor = true;
            this.btn_Exit.Click += new System.EventHandler(this.btn_Exit_Click);
            // 
            // listBox
            // 
            this.listBox.FormattingEnabled = true;
            this.listBox.Location = new System.Drawing.Point(78, 256);
            this.listBox.Name = "listBox";
            this.listBox.Size = new System.Drawing.Size(237, 173);
            this.listBox.TabIndex = 6;
            this.listBox.SelectedIndexChanged += new System.EventHandler(this.listBox_SelectedIndexChanged);
            // 
            // btn_C_to_F
            // 
            this.btn_C_to_F.Location = new System.Drawing.Point(133, 123);
            this.btn_C_to_F.Margin = new System.Windows.Forms.Padding(2);
            this.btn_C_to_F.Name = "btn_C_to_F";
            this.btn_C_to_F.Size = new System.Drawing.Size(122, 19);
            this.btn_C_to_F.TabIndex = 7;
            this.btn_C_to_F.Text = "Celcius to Farenheit";
            this.btn_C_to_F.UseVisualStyleBackColor = true;
            this.btn_C_to_F.Click += new System.EventHandler(this.btn_C_to_F_Click);
            // 
            // btn_CM_to_Feet
            // 
            this.btn_CM_to_Feet.Location = new System.Drawing.Point(133, 160);
            this.btn_CM_to_Feet.Margin = new System.Windows.Forms.Padding(2);
            this.btn_CM_to_Feet.Name = "btn_CM_to_Feet";
            this.btn_CM_to_Feet.Size = new System.Drawing.Size(122, 19);
            this.btn_CM_to_Feet.TabIndex = 8;
            this.btn_CM_to_Feet.Text = "Centimeter to Feet";
            this.btn_CM_to_Feet.UseVisualStyleBackColor = true;
            this.btn_CM_to_Feet.Click += new System.EventHandler(this.btn_CM_to_Feet_Click);
            // 
            // btn_KM_to_Miles
            // 
            this.btn_KM_to_Miles.Location = new System.Drawing.Point(133, 202);
            this.btn_KM_to_Miles.Margin = new System.Windows.Forms.Padding(2);
            this.btn_KM_to_Miles.Name = "btn_KM_to_Miles";
            this.btn_KM_to_Miles.Size = new System.Drawing.Size(122, 19);
            this.btn_KM_to_Miles.TabIndex = 9;
            this.btn_KM_to_Miles.Text = "Kilometers to Miles";
            this.btn_KM_to_Miles.UseVisualStyleBackColor = true;
            this.btn_KM_to_Miles.Click += new System.EventHandler(this.btn_KM_to_Miles_Click);
            // 
            // frm_Main
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(402, 476);
            this.Controls.Add(this.btn_KM_to_Miles);
            this.Controls.Add(this.btn_CM_to_Feet);
            this.Controls.Add(this.btn_C_to_F);
            this.Controls.Add(this.listBox);
            this.Controls.Add(this.btn_Exit);
            this.Controls.Add(this.btn_M_to_Feet);
            this.Controls.Add(this.btn_CM_to_Inches);
            this.Controls.Add(this.txt_UnitOfMeasure);
            this.Controls.Add(this.lbl_UofM);
            this.Margin = new System.Windows.Forms.Padding(2);
            this.Name = "frm_Main";
            this.Text = "ATCA Gas Converter";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label lbl_UofM;
        private System.Windows.Forms.TextBox txt_UnitOfMeasure;
        private System.Windows.Forms.Button btn_CM_to_Inches;
        private System.Windows.Forms.Button btn_M_to_Feet;
        private System.Windows.Forms.Button btn_Exit;
        private System.Windows.Forms.ListBox listBox;
        private System.Windows.Forms.Button btn_C_to_F;
        private System.Windows.Forms.Button btn_CM_to_Feet;
        private System.Windows.Forms.Button btn_KM_to_Miles;
    }
}

namespace Encaps
{
    partial class Form1
    {
        /// <summary>
        ///  Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        ///  Clean up any resources being used.
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
        ///  Required method for Designer support - do not modify
        ///  the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            label1 = new Label();
            btnEnvoyer = new Button();
            txtEmployeeID = new TextBox();
            label2 = new Label();
            txtEmployeeName = new TextBox();
            label3 = new Label();
            txtEmployeeAge = new TextBox();
            label4 = new Label();
            txtEmployeePosition = new TextBox();
            SuspendLayout();
            // 
            // label1
            // 
            label1.AutoSize = true;
            label1.Font = new Font("Segoe UI", 12F, FontStyle.Bold);
            label1.Location = new Point(67, 57);
            label1.Name = "label1";
            label1.Size = new Size(131, 28);
            label1.TabIndex = 0;
            label1.Text = "Employee ID";
            // 
            // btnEnvoyer
            // 
            btnEnvoyer.Font = new Font("Segoe UI", 12F, FontStyle.Bold);
            btnEnvoyer.Location = new Point(150, 319);
            btnEnvoyer.Name = "btnEnvoyer";
            btnEnvoyer.Size = new Size(161, 52);
            btnEnvoyer.TabIndex = 1;
            btnEnvoyer.Text = "Envoyer";
            btnEnvoyer.UseVisualStyleBackColor = true;
            btnEnvoyer.Click += this.btnEnvoyer_Click;
            // 
            // txtEmployeeID
            // 
            txtEmployeeID.Location = new Point(260, 58);
            txtEmployeeID.Name = "txtEmployeeID";
            txtEmployeeID.Size = new Size(125, 27);
            txtEmployeeID.TabIndex = 2;
            // 
            // label2
            // 
            label2.AutoSize = true;
            label2.Font = new Font("Segoe UI", 12F, FontStyle.Bold);
            label2.Location = new Point(67, 109);
            label2.Name = "label2";
            label2.Size = new Size(166, 28);
            label2.TabIndex = 0;
            label2.Text = "Employee Name";
            // 
            // txtEmployeeName
            // 
            txtEmployeeName.Location = new Point(260, 109);
            txtEmployeeName.Name = "txtEmployeeName";
            txtEmployeeName.Size = new Size(125, 27);
            txtEmployeeName.TabIndex = 2;
            // 
            // label3
            // 
            label3.AutoSize = true;
            label3.Font = new Font("Segoe UI", 12F, FontStyle.Bold);
            label3.Location = new Point(67, 172);
            label3.Name = "label3";
            label3.Size = new Size(147, 28);
            label3.TabIndex = 0;
            label3.Text = "Employee Age";
            // 
            // txtEmployeeAge
            // 
            txtEmployeeAge.Location = new Point(260, 176);
            txtEmployeeAge.Name = "txtEmployeeAge";
            txtEmployeeAge.Size = new Size(125, 27);
            txtEmployeeAge.TabIndex = 2;
            // 
            // label4
            // 
            label4.AutoSize = true;
            label4.Font = new Font("Segoe UI", 12F, FontStyle.Bold);
            label4.Location = new Point(67, 246);
            label4.Name = "label4";
            label4.Size = new Size(186, 28);
            label4.TabIndex = 0;
            label4.Text = "Employee Position";
            // 
            // txtEmployeePosition
            // 
            txtEmployeePosition.Location = new Point(260, 247);
            txtEmployeePosition.Name = "txtEmployeePosition";
            txtEmployeePosition.Size = new Size(125, 27);
            txtEmployeePosition.TabIndex = 2;
            // 
            // Form1
            // 
            AutoScaleDimensions = new SizeF(8F, 20F);
            AutoScaleMode = AutoScaleMode.Font;
            ClientSize = new Size(463, 415);
            Controls.Add(txtEmployeePosition);
            Controls.Add(txtEmployeeAge);
            Controls.Add(label4);
            Controls.Add(txtEmployeeName);
            Controls.Add(label3);
            Controls.Add(txtEmployeeID);
            Controls.Add(label2);
            Controls.Add(btnEnvoyer);
            Controls.Add(label1);
            Name = "Form1";
            Text = "Form1";
            ResumeLayout(false);
            PerformLayout();
        }

        #endregion

        private Label label1;
        private Button btnEnvoyer;
        private TextBox txtEmployeeID;
        private Label label2;
        private TextBox txtEmployeeName;
        private Label label3;
        private TextBox txtEmployeeAge;
        private Label label4;
        private TextBox txtEmployeePosition;
    }
}
namespace SurveyFiller
{
    partial class FormSMCheck
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
            this.labelSMCheckErrorText = new System.Windows.Forms.Label();
            this.btnCheckSMSubmit = new System.Windows.Forms.Button();
            this.textBoxVC = new System.Windows.Forms.TextBox();
            this.label7 = new System.Windows.Forms.Label();
            this.labelSMCheckSeq = new System.Windows.Forms.Label();
            this.labelSMCheckUserName = new System.Windows.Forms.Label();
            this.label6 = new System.Windows.Forms.Label();
            this.label5 = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.btnCancel = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // labelSMCheckErrorText
            // 
            this.labelSMCheckErrorText.AutoSize = true;
            this.labelSMCheckErrorText.Font = new System.Drawing.Font("微软雅黑", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(134)));
            this.labelSMCheckErrorText.ForeColor = System.Drawing.Color.Red;
            this.labelSMCheckErrorText.Location = new System.Drawing.Point(13, 95);
            this.labelSMCheckErrorText.Name = "labelSMCheckErrorText";
            this.labelSMCheckErrorText.Size = new System.Drawing.Size(0, 17);
            this.labelSMCheckErrorText.TabIndex = 18;
            // 
            // btnCheckSMSubmit
            // 
            this.btnCheckSMSubmit.Enabled = false;
            this.btnCheckSMSubmit.Font = new System.Drawing.Font("微软雅黑", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(134)));
            this.btnCheckSMSubmit.Location = new System.Drawing.Point(185, 63);
            this.btnCheckSMSubmit.Name = "btnCheckSMSubmit";
            this.btnCheckSMSubmit.Size = new System.Drawing.Size(52, 25);
            this.btnCheckSMSubmit.TabIndex = 17;
            this.btnCheckSMSubmit.Text = "确定";
            this.btnCheckSMSubmit.UseVisualStyleBackColor = true;
            this.btnCheckSMSubmit.Click += new System.EventHandler(this.btnCheckSMSubmit_Click);
            // 
            // textBoxVC
            // 
            this.textBoxVC.Font = new System.Drawing.Font("Consolas", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.textBoxVC.Location = new System.Drawing.Point(106, 62);
            this.textBoxVC.Name = "textBoxVC";
            this.textBoxVC.Size = new System.Drawing.Size(68, 26);
            this.textBoxVC.TabIndex = 15;
            this.textBoxVC.TextAlign = System.Windows.Forms.HorizontalAlignment.Center;
            this.textBoxVC.TextChanged += new System.EventHandler(this.textBoxVC_TextChanged);
            // 
            // label7
            // 
            this.label7.AutoSize = true;
            this.label7.Font = new System.Drawing.Font("微软雅黑", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(134)));
            this.label7.Location = new System.Drawing.Point(12, 63);
            this.label7.Name = "label7";
            this.label7.Size = new System.Drawing.Size(90, 21);
            this.label7.TabIndex = 14;
            this.label7.Text = "短信验证码";
            // 
            // labelSMCheckSeq
            // 
            this.labelSMCheckSeq.AutoSize = true;
            this.labelSMCheckSeq.Font = new System.Drawing.Font("微软雅黑", 9F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(134)));
            this.labelSMCheckSeq.ForeColor = System.Drawing.Color.MediumBlue;
            this.labelSMCheckSeq.Location = new System.Drawing.Point(272, 40);
            this.labelSMCheckSeq.Name = "labelSMCheckSeq";
            this.labelSMCheckSeq.Size = new System.Drawing.Size(0, 17);
            this.labelSMCheckSeq.TabIndex = 13;
            // 
            // labelSMCheckUserName
            // 
            this.labelSMCheckUserName.AutoSize = true;
            this.labelSMCheckUserName.Font = new System.Drawing.Font("微软雅黑", 9F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(134)));
            this.labelSMCheckUserName.ForeColor = System.Drawing.Color.MediumBlue;
            this.labelSMCheckUserName.Location = new System.Drawing.Point(103, 40);
            this.labelSMCheckUserName.Name = "labelSMCheckUserName";
            this.labelSMCheckUserName.Size = new System.Drawing.Size(0, 17);
            this.labelSMCheckUserName.TabIndex = 12;
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Font = new System.Drawing.Font("微软雅黑", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(134)));
            this.label6.Location = new System.Drawing.Point(182, 40);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(92, 17);
            this.label6.TabIndex = 11;
            this.label6.Text = "验证码序列号：";
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Font = new System.Drawing.Font("微软雅黑", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(134)));
            this.label5.Location = new System.Drawing.Point(13, 40);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(92, 17);
            this.label5.TabIndex = 10;
            this.label5.Text = "乘车人用户名：";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("微软雅黑", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(134)));
            this.label1.Location = new System.Drawing.Point(12, 9);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(316, 21);
            this.label1.TabIndex = 9;
            this.label1.Text = "请在15分钟内输入相应手机上收到的验证码";
            // 
            // btnCancel
            // 
            this.btnCancel.Font = new System.Drawing.Font("微软雅黑", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(134)));
            this.btnCancel.Location = new System.Drawing.Point(243, 63);
            this.btnCancel.Name = "btnCancel";
            this.btnCancel.Size = new System.Drawing.Size(52, 25);
            this.btnCancel.TabIndex = 19;
            this.btnCancel.Text = "放弃";
            this.btnCancel.UseVisualStyleBackColor = true;
            this.btnCancel.Click += new System.EventHandler(this.btnCancel_Click);
            // 
            // FormSMCheck
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 12F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(346, 124);
            this.Controls.Add(this.btnCancel);
            this.Controls.Add(this.labelSMCheckErrorText);
            this.Controls.Add(this.btnCheckSMSubmit);
            this.Controls.Add(this.textBoxVC);
            this.Controls.Add(this.label7);
            this.Controls.Add(this.labelSMCheckSeq);
            this.Controls.Add(this.labelSMCheckUserName);
            this.Controls.Add(this.label6);
            this.Controls.Add(this.label5);
            this.Controls.Add(this.label1);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedDialog;
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "FormSMCheck";
            this.ShowIcon = false;
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterParent;
            this.Text = "短信验证码校验";
            this.TopMost = true;
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label labelSMCheckErrorText;
        private System.Windows.Forms.Button btnCheckSMSubmit;
        private System.Windows.Forms.TextBox textBoxVC;
        private System.Windows.Forms.Label label7;
        private System.Windows.Forms.Label labelSMCheckSeq;
        private System.Windows.Forms.Label labelSMCheckUserName;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Button btnCancel;
    }
}
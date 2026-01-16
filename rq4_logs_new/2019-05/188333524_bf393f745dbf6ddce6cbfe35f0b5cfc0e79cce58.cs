namespace TeaBagMaker
{
    partial class Form1
    {
        /// <summary>
        /// 필수 디자이너 변수입니다.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// 사용 중인 모든 리소스를 정리합니다.
        /// </summary>
        /// <param name="disposing">관리되는 리소스를 삭제해야 하면 true이고, 그렇지 않으면 false입니다.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form 디자이너에서 생성한 코드

        /// <summary>
        /// 디자이너 지원에 필요한 메서드입니다. 
        /// 이 메서드의 내용을 코드 편집기로 수정하지 마세요.
        /// </summary>
        private void InitializeComponent()
        {
            this.cbTea = new System.Windows.Forms.ComboBox();
            this.lblTime = new System.Windows.Forms.Label();
            this.btn = new System.Windows.Forms.Button();
            this.tbCount = new System.Windows.Forms.TextBox();
            this.label2 = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // cbTea
            // 
            this.cbTea.FormattingEnabled = true;
            this.cbTea.Location = new System.Drawing.Point(12, 9);
            this.cbTea.Name = "cbTea";
            this.cbTea.Size = new System.Drawing.Size(121, 20);
            this.cbTea.TabIndex = 0;
            // 
            // lblTime
            // 
            this.lblTime.AutoSize = true;
            this.lblTime.Location = new System.Drawing.Point(139, 9);
            this.lblTime.Name = "lblTime";
            this.lblTime.Size = new System.Drawing.Size(41, 12);
            this.lblTime.TabIndex = 1;
            this.lblTime.Text = "시간 : ";
            // 
            // btn
            // 
            this.btn.Location = new System.Drawing.Point(12, 39);
            this.btn.Name = "btn";
            this.btn.Size = new System.Drawing.Size(166, 57);
            this.btn.TabIndex = 2;
            this.btn.Text = "담그기!";
            this.btn.UseVisualStyleBackColor = true;
            // 
            // tbCount
            // 
            this.tbCount.Location = new System.Drawing.Point(12, 113);
            this.tbCount.Name = "tbCount";
            this.tbCount.Size = new System.Drawing.Size(166, 21);
            this.tbCount.TabIndex = 3;
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(118, 147);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(131, 12);
            this.label2.TabIndex = 4;
            this.label2.Text = "2019.05.24 3207-박혜정";
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(7F, 12F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(261, 171);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.tbCount);
            this.Controls.Add(this.btn);
            this.Controls.Add(this.lblTime);
            this.Controls.Add(this.cbTea);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedSingle;
            this.MaximizeBox = false;
            this.Name = "Form1";
            this.Text = "티 담그기";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.ComboBox cbTea;
        private System.Windows.Forms.Label lblTime;
        private System.Windows.Forms.Button btn;
        private System.Windows.Forms.TextBox tbCount;
        private System.Windows.Forms.Label label2;
    }
}

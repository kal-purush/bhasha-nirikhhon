namespace ZExcelUpDown
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
        /// 이 메서드의 내용을 코드 편집기로 수정하지 마십시오.
        /// </summary>
        private void InitializeComponent()
        {
            this.btnXlsUp = new System.Windows.Forms.Button();
            this.btnXlsDown = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // btnXlsUp
            // 
            this.btnXlsUp.Location = new System.Drawing.Point(13, 13);
            this.btnXlsUp.Name = "btnXlsUp";
            this.btnXlsUp.Size = new System.Drawing.Size(118, 23);
            this.btnXlsUp.TabIndex = 0;
            this.btnXlsUp.Text = "엑셀 업로드";
            this.btnXlsUp.UseVisualStyleBackColor = true;
            // 
            // btnXlsDown
            // 
            this.btnXlsDown.Location = new System.Drawing.Point(13, 42);
            this.btnXlsDown.Name = "btnXlsDown";
            this.btnXlsDown.Size = new System.Drawing.Size(118, 23);
            this.btnXlsDown.TabIndex = 0;
            this.btnXlsDown.Text = "엑셀양식 다운로드";
            this.btnXlsDown.UseVisualStyleBackColor = true;
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(7F, 12F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(284, 262);
            this.Controls.Add(this.btnXlsDown);
            this.Controls.Add(this.btnXlsUp);
            this.Name = "Form1";
            this.Text = "Form1";
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Button btnXlsUp;
        private System.Windows.Forms.Button btnXlsDown;
    }
}

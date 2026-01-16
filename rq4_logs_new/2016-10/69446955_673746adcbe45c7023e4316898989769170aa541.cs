namespace KincoP2L
{
    partial class LedFlashParamPicker
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
            this.spFlashCount = new DevExpress.XtraEditors.SpinEdit();
            this.labelControl1 = new DevExpress.XtraEditors.LabelControl();
            this.spOnTime = new DevExpress.XtraEditors.SpinEdit();
            this.labelControl2 = new DevExpress.XtraEditors.LabelControl();
            this.labelControl3 = new DevExpress.XtraEditors.LabelControl();
            this.labelControl4 = new DevExpress.XtraEditors.LabelControl();
            this.labelControl5 = new DevExpress.XtraEditors.LabelControl();
            this.spOffTime = new DevExpress.XtraEditors.SpinEdit();
            this.btOK = new DevExpress.XtraEditors.SimpleButton();
            ((System.ComponentModel.ISupportInitialize)(this.spFlashCount.Properties)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.spOnTime.Properties)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.spOffTime.Properties)).BeginInit();
            this.SuspendLayout();
            // 
            // spFlashCount
            // 
            this.spFlashCount.EditValue = new decimal(new int[] {
            10,
            0,
            0,
            0});
            this.spFlashCount.Location = new System.Drawing.Point(107, 27);
            this.spFlashCount.Name = "spFlashCount";
            this.spFlashCount.Properties.Buttons.AddRange(new DevExpress.XtraEditors.Controls.EditorButton[] {
            new DevExpress.XtraEditors.Controls.EditorButton()});
            this.spFlashCount.Properties.MaxValue = new decimal(new int[] {
            65535,
            0,
            0,
            0});
            this.spFlashCount.Properties.MinValue = new decimal(new int[] {
            1,
            0,
            0,
            0});
            this.spFlashCount.Size = new System.Drawing.Size(110, 21);
            this.spFlashCount.TabIndex = 0;
            // 
            // labelControl1
            // 
            this.labelControl1.Location = new System.Drawing.Point(23, 30);
            this.labelControl1.Name = "labelControl1";
            this.labelControl1.Size = new System.Drawing.Size(48, 14);
            this.labelControl1.TabIndex = 1;
            this.labelControl1.Text = "閃爍次數";
            // 
            // spOnTime
            // 
            this.spOnTime.EditValue = new decimal(new int[] {
            1000,
            0,
            0,
            0});
            this.spOnTime.Location = new System.Drawing.Point(107, 65);
            this.spOnTime.Name = "spOnTime";
            this.spOnTime.Properties.Buttons.AddRange(new DevExpress.XtraEditors.Controls.EditorButton[] {
            new DevExpress.XtraEditors.Controls.EditorButton()});
            this.spOnTime.Properties.MaxValue = new decimal(new int[] {
            65535,
            0,
            0,
            0});
            this.spOnTime.Properties.MinValue = new decimal(new int[] {
            1,
            0,
            0,
            0});
            this.spOnTime.Size = new System.Drawing.Size(110, 21);
            this.spOnTime.TabIndex = 2;
            // 
            // labelControl2
            // 
            this.labelControl2.Location = new System.Drawing.Point(23, 68);
            this.labelControl2.Name = "labelControl2";
            this.labelControl2.Size = new System.Drawing.Size(72, 14);
            this.labelControl2.TabIndex = 3;
            this.labelControl2.Text = "亮燈持續時間";
            // 
            // labelControl3
            // 
            this.labelControl3.Location = new System.Drawing.Point(224, 68);
            this.labelControl3.Name = "labelControl3";
            this.labelControl3.Size = new System.Drawing.Size(24, 14);
            this.labelControl3.TabIndex = 4;
            this.labelControl3.Text = "毫秒";
            // 
            // labelControl4
            // 
            this.labelControl4.Location = new System.Drawing.Point(224, 105);
            this.labelControl4.Name = "labelControl4";
            this.labelControl4.Size = new System.Drawing.Size(24, 14);
            this.labelControl4.TabIndex = 7;
            this.labelControl4.Text = "毫秒";
            // 
            // labelControl5
            // 
            this.labelControl5.Location = new System.Drawing.Point(23, 105);
            this.labelControl5.Name = "labelControl5";
            this.labelControl5.Size = new System.Drawing.Size(72, 14);
            this.labelControl5.TabIndex = 6;
            this.labelControl5.Text = "滅燈持續時間";
            // 
            // spOffTime
            // 
            this.spOffTime.EditValue = new decimal(new int[] {
            500,
            0,
            0,
            0});
            this.spOffTime.Location = new System.Drawing.Point(107, 102);
            this.spOffTime.Name = "spOffTime";
            this.spOffTime.Properties.Buttons.AddRange(new DevExpress.XtraEditors.Controls.EditorButton[] {
            new DevExpress.XtraEditors.Controls.EditorButton()});
            this.spOffTime.Properties.MaxValue = new decimal(new int[] {
            65535,
            0,
            0,
            0});
            this.spOffTime.Properties.MinValue = new decimal(new int[] {
            1,
            0,
            0,
            0});
            this.spOffTime.Size = new System.Drawing.Size(110, 21);
            this.spOffTime.TabIndex = 5;
            // 
            // btOK
            // 
            this.btOK.DialogResult = System.Windows.Forms.DialogResult.OK;
            this.btOK.Location = new System.Drawing.Point(142, 143);
            this.btOK.Name = "btOK";
            this.btOK.Size = new System.Drawing.Size(75, 38);
            this.btOK.TabIndex = 8;
            this.btOK.Text = "確    定";
            // 
            // LedFlashParamPicker
            // 
            this.AcceptButton = this.btOK;
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 12F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(257, 193);
            this.Controls.Add(this.btOK);
            this.Controls.Add(this.labelControl4);
            this.Controls.Add(this.labelControl5);
            this.Controls.Add(this.spOffTime);
            this.Controls.Add(this.labelControl3);
            this.Controls.Add(this.labelControl2);
            this.Controls.Add(this.spOnTime);
            this.Controls.Add(this.labelControl1);
            this.Controls.Add(this.spFlashCount);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedToolWindow;
            this.Name = "LedFlashParamPicker";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterParent;
            this.Text = "參數設定";
            ((System.ComponentModel.ISupportInitialize)(this.spFlashCount.Properties)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.spOnTime.Properties)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.spOffTime.Properties)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private DevExpress.XtraEditors.SpinEdit spFlashCount;
        private DevExpress.XtraEditors.LabelControl labelControl1;
        private DevExpress.XtraEditors.SpinEdit spOnTime;
        private DevExpress.XtraEditors.LabelControl labelControl2;
        private DevExpress.XtraEditors.LabelControl labelControl3;
        private DevExpress.XtraEditors.LabelControl labelControl4;
        private DevExpress.XtraEditors.LabelControl labelControl5;
        private DevExpress.XtraEditors.SpinEdit spOffTime;
        private DevExpress.XtraEditors.SimpleButton btOK;
    }
}
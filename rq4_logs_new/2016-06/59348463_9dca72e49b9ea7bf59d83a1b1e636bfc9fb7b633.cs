namespace SLFA
{
    partial class PortSettings
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
            this.portGroupBox = new System.Windows.Forms.GroupBox();
            this.portComboBox = new System.Windows.Forms.ComboBox();
            this.bitRatesComboBox = new System.Windows.Forms.ComboBox();
            this.bitRatesGroupBox = new System.Windows.Forms.GroupBox();
            this.dataBitsComboBox = new System.Windows.Forms.ComboBox();
            this.dataBitsGroupBox = new System.Windows.Forms.GroupBox();
            this.stopBitsComboBox = new System.Windows.Forms.ComboBox();
            this.stopBitsGroupBox = new System.Windows.Forms.GroupBox();
            this.comboBox4 = new System.Windows.Forms.ComboBox();
            this.parityGroupBox = new System.Windows.Forms.GroupBox();
            this.handshakingComboBox = new System.Windows.Forms.ComboBox();
            this.handshakingGroupBox = new System.Windows.Forms.GroupBox();
            this.openPortOnStartupCheckBox = new System.Windows.Forms.CheckBox();
            this.statusGroupBox = new System.Windows.Forms.GroupBox();
            this.statusProgressBar = new System.Windows.Forms.ProgressBar();
            this.openPortButton = new System.Windows.Forms.Button();
            this.closePortButton = new System.Windows.Forms.Button();
            this.portGroupBox.SuspendLayout();
            this.bitRatesGroupBox.SuspendLayout();
            this.dataBitsGroupBox.SuspendLayout();
            this.stopBitsGroupBox.SuspendLayout();
            this.parityGroupBox.SuspendLayout();
            this.handshakingGroupBox.SuspendLayout();
            this.statusGroupBox.SuspendLayout();
            this.SuspendLayout();
            // 
            // portGroupBox
            // 
            this.portGroupBox.Controls.Add(this.portComboBox);
            this.portGroupBox.Location = new System.Drawing.Point(19, 14);
            this.portGroupBox.Name = "portGroupBox";
            this.portGroupBox.Size = new System.Drawing.Size(150, 60);
            this.portGroupBox.TabIndex = 0;
            this.portGroupBox.TabStop = false;
            this.portGroupBox.Text = "COM-порт";
            this.portGroupBox.UseCompatibleTextRendering = true;
            // 
            // portComboBox
            // 
            this.portComboBox.FormattingEnabled = true;
            this.portComboBox.Location = new System.Drawing.Point(7, 23);
            this.portComboBox.Name = "portComboBox";
            this.portComboBox.Size = new System.Drawing.Size(137, 21);
            this.portComboBox.TabIndex = 0;
            // 
            // bitRatesComboBox
            // 
            this.bitRatesComboBox.FormattingEnabled = true;
            this.bitRatesComboBox.Location = new System.Drawing.Point(7, 23);
            this.bitRatesComboBox.Name = "bitRatesComboBox";
            this.bitRatesComboBox.Size = new System.Drawing.Size(137, 21);
            this.bitRatesComboBox.TabIndex = 0;
            // 
            // bitRatesGroupBox
            // 
            this.bitRatesGroupBox.Controls.Add(this.bitRatesComboBox);
            this.bitRatesGroupBox.Location = new System.Drawing.Point(194, 14);
            this.bitRatesGroupBox.Name = "bitRatesGroupBox";
            this.bitRatesGroupBox.Size = new System.Drawing.Size(150, 60);
            this.bitRatesGroupBox.TabIndex = 1;
            this.bitRatesGroupBox.TabStop = false;
            this.bitRatesGroupBox.Text = "Скорость передачи бит/с";
            this.bitRatesGroupBox.UseCompatibleTextRendering = true;
            // 
            // dataBitsComboBox
            // 
            this.dataBitsComboBox.FormattingEnabled = true;
            this.dataBitsComboBox.Location = new System.Drawing.Point(7, 23);
            this.dataBitsComboBox.Name = "dataBitsComboBox";
            this.dataBitsComboBox.Size = new System.Drawing.Size(137, 21);
            this.dataBitsComboBox.TabIndex = 0;
            // 
            // dataBitsGroupBox
            // 
            this.dataBitsGroupBox.Controls.Add(this.dataBitsComboBox);
            this.dataBitsGroupBox.Location = new System.Drawing.Point(19, 83);
            this.dataBitsGroupBox.Name = "dataBitsGroupBox";
            this.dataBitsGroupBox.Size = new System.Drawing.Size(150, 60);
            this.dataBitsGroupBox.TabIndex = 1;
            this.dataBitsGroupBox.TabStop = false;
            this.dataBitsGroupBox.Text = "Биты данных";
            this.dataBitsGroupBox.UseCompatibleTextRendering = true;
            // 
            // stopBitsComboBox
            // 
            this.stopBitsComboBox.FormattingEnabled = true;
            this.stopBitsComboBox.Location = new System.Drawing.Point(7, 23);
            this.stopBitsComboBox.Name = "stopBitsComboBox";
            this.stopBitsComboBox.Size = new System.Drawing.Size(137, 21);
            this.stopBitsComboBox.TabIndex = 0;
            // 
            // stopBitsGroupBox
            // 
            this.stopBitsGroupBox.Controls.Add(this.stopBitsComboBox);
            this.stopBitsGroupBox.Location = new System.Drawing.Point(194, 83);
            this.stopBitsGroupBox.Name = "stopBitsGroupBox";
            this.stopBitsGroupBox.Size = new System.Drawing.Size(150, 60);
            this.stopBitsGroupBox.TabIndex = 2;
            this.stopBitsGroupBox.TabStop = false;
            this.stopBitsGroupBox.Text = "Стоп-биты";
            this.stopBitsGroupBox.UseCompatibleTextRendering = true;
            // 
            // comboBox4
            // 
            this.comboBox4.FormattingEnabled = true;
            this.comboBox4.Location = new System.Drawing.Point(7, 23);
            this.comboBox4.Name = "comboBox4";
            this.comboBox4.Size = new System.Drawing.Size(137, 21);
            this.comboBox4.TabIndex = 0;
            // 
            // parityGroupBox
            // 
            this.parityGroupBox.Controls.Add(this.comboBox4);
            this.parityGroupBox.Location = new System.Drawing.Point(19, 153);
            this.parityGroupBox.Name = "parityGroupBox";
            this.parityGroupBox.Size = new System.Drawing.Size(150, 60);
            this.parityGroupBox.TabIndex = 3;
            this.parityGroupBox.TabStop = false;
            this.parityGroupBox.Text = "Четность";
            this.parityGroupBox.UseCompatibleTextRendering = true;
            // 
            // handshakingComboBox
            // 
            this.handshakingComboBox.FormattingEnabled = true;
            this.handshakingComboBox.Location = new System.Drawing.Point(7, 23);
            this.handshakingComboBox.Name = "handshakingComboBox";
            this.handshakingComboBox.Size = new System.Drawing.Size(137, 21);
            this.handshakingComboBox.TabIndex = 0;
            // 
            // handshakingGroupBox
            // 
            this.handshakingGroupBox.Controls.Add(this.handshakingComboBox);
            this.handshakingGroupBox.Location = new System.Drawing.Point(194, 153);
            this.handshakingGroupBox.Name = "handshakingGroupBox";
            this.handshakingGroupBox.Size = new System.Drawing.Size(150, 60);
            this.handshakingGroupBox.TabIndex = 1;
            this.handshakingGroupBox.TabStop = false;
            this.handshakingGroupBox.Text = "Синхронизация обмена";
            this.handshakingGroupBox.UseCompatibleTextRendering = true;
            // 
            // openPortOnStartupCheckBox
            // 
            this.openPortOnStartupCheckBox.AutoSize = true;
            this.openPortOnStartupCheckBox.Location = new System.Drawing.Point(19, 341);
            this.openPortOnStartupCheckBox.Name = "openPortOnStartupCheckBox";
            this.openPortOnStartupCheckBox.Size = new System.Drawing.Size(223, 17);
            this.openPortOnStartupCheckBox.TabIndex = 4;
            this.openPortOnStartupCheckBox.Text = "Открыть порт при запуске программы";
            this.openPortOnStartupCheckBox.UseVisualStyleBackColor = true;
            // 
            // statusGroupBox
            // 
            this.statusGroupBox.Controls.Add(this.statusProgressBar);
            this.statusGroupBox.Location = new System.Drawing.Point(19, 225);
            this.statusGroupBox.Name = "statusGroupBox";
            this.statusGroupBox.Size = new System.Drawing.Size(325, 59);
            this.statusGroupBox.TabIndex = 5;
            this.statusGroupBox.TabStop = false;
            this.statusGroupBox.Text = "Статус ";
            // 
            // statusProgressBar
            // 
            this.statusProgressBar.Location = new System.Drawing.Point(7, 25);
            this.statusProgressBar.Name = "statusProgressBar";
            this.statusProgressBar.Size = new System.Drawing.Size(311, 16);
            this.statusProgressBar.TabIndex = 0;
            // 
            // openPortButton
            // 
            this.openPortButton.Location = new System.Drawing.Point(61, 290);
            this.openPortButton.Name = "openPortButton";
            this.openPortButton.Size = new System.Drawing.Size(102, 28);
            this.openPortButton.TabIndex = 6;
            this.openPortButton.Text = "Открыть порт";
            this.openPortButton.UseVisualStyleBackColor = true;
            // 
            // closePortButton
            // 
            this.closePortButton.Location = new System.Drawing.Point(201, 290);
            this.closePortButton.Name = "closePortButton";
            this.closePortButton.Size = new System.Drawing.Size(102, 28);
            this.closePortButton.TabIndex = 7;
            this.closePortButton.Text = "Закрыть порт";
            this.closePortButton.UseVisualStyleBackColor = true;
            // 
            // PortSettings
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(365, 408);
            this.Controls.Add(this.closePortButton);
            this.Controls.Add(this.openPortButton);
            this.Controls.Add(this.statusGroupBox);
            this.Controls.Add(this.openPortOnStartupCheckBox);
            this.Controls.Add(this.handshakingGroupBox);
            this.Controls.Add(this.parityGroupBox);
            this.Controls.Add(this.stopBitsGroupBox);
            this.Controls.Add(this.dataBitsGroupBox);
            this.Controls.Add(this.bitRatesGroupBox);
            this.Controls.Add(this.portGroupBox);
            this.Name = "PortSettings";
            this.Text = "Настройки порта связи";
            this.portGroupBox.ResumeLayout(false);
            this.bitRatesGroupBox.ResumeLayout(false);
            this.dataBitsGroupBox.ResumeLayout(false);
            this.stopBitsGroupBox.ResumeLayout(false);
            this.parityGroupBox.ResumeLayout(false);
            this.handshakingGroupBox.ResumeLayout(false);
            this.statusGroupBox.ResumeLayout(false);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.GroupBox portGroupBox;
        private System.Windows.Forms.ComboBox portComboBox;
        private System.Windows.Forms.ComboBox bitRatesComboBox;
        private System.Windows.Forms.GroupBox bitRatesGroupBox;
        private System.Windows.Forms.ComboBox dataBitsComboBox;
        private System.Windows.Forms.GroupBox dataBitsGroupBox;
        private System.Windows.Forms.ComboBox stopBitsComboBox;
        private System.Windows.Forms.GroupBox stopBitsGroupBox;
        private System.Windows.Forms.ComboBox comboBox4;
        private System.Windows.Forms.GroupBox parityGroupBox;
        private System.Windows.Forms.ComboBox handshakingComboBox;
        private System.Windows.Forms.GroupBox handshakingGroupBox;
        private System.Windows.Forms.CheckBox openPortOnStartupCheckBox;
        private System.Windows.Forms.GroupBox statusGroupBox;
        private System.Windows.Forms.ProgressBar statusProgressBar;
        private System.Windows.Forms.Button openPortButton;
        private System.Windows.Forms.Button closePortButton;
    }
}
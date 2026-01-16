namespace DatabaseComparisonForm
{
    partial class MainForm
    {
        /// <summary>
        /// Обязательная переменная конструктора.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Освободить все используемые ресурсы.
        /// </summary>
        /// <param name="disposing">истинно, если управляемый ресурс должен быть удален; иначе ложно.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Код, автоматически созданный конструктором форм Windows

        /// <summary>
        /// Требуемый метод для поддержки конструктора — не изменяйте 
        /// содержимое этого метода с помощью редактора кода.
        /// </summary>
        private void InitializeComponent()
        {
            this._userControlConnect = new DatabaseComparisonForm.UserControls.UserControlConnect();
            this._userControlProgress = new DatabaseComparisonForm.UserControls.UserControlProgress();
            this._userControlComparisonOutput = new DatabaseComparisonForm.UserControls.UserControlComparisonOutput();
            this.SuspendLayout();
            // 
            // _userControlConnect
            // 
            this._userControlConnect.Connect = false;
            this._userControlConnect.Location = new System.Drawing.Point(12, 12);
            this._userControlConnect.Name = "_userControlConnect";
            this._userControlConnect.Size = new System.Drawing.Size(479, 169);
            this._userControlConnect.TabIndex = 0;
            this._userControlConnect.EnabledChanged += new System.EventHandler(this._userControlConnect_EnabledChanged);
            // 
            // _userControlProgress
            // 
            this._userControlProgress.ConnectorSQLiteSourse = null;
            this._userControlProgress.ConnectorSQLiteTarget = null;
            this._userControlProgress.Enabled = false;
            this._userControlProgress.Location = new System.Drawing.Point(12, 12);
            this._userControlProgress.Name = "_userControlProgress";
            this._userControlProgress.Progress = false;
            this._userControlProgress.Size = new System.Drawing.Size(498, 251);
            this._userControlProgress.TabIndex = 1;
            this._userControlProgress.Visible = false;
            this._userControlProgress.EnabledChanged += new System.EventHandler(this._userControlProgress_EnabledChanged);
            // 
            // _userControlComparisonOutput
            // 
            this._userControlComparisonOutput.Enabled = false;
            this._userControlComparisonOutput.Location = new System.Drawing.Point(12, 12);
            this._userControlComparisonOutput.Name = "_userControlComparisonOutput";
            this._userControlComparisonOutput.Output = false;
            this._userControlComparisonOutput.Size = new System.Drawing.Size(552, 360);
            this._userControlComparisonOutput.TabIndex = 2;
            this._userControlComparisonOutput.Visible = false;
            this._userControlComparisonOutput.EnabledChanged += new System.EventHandler(this._userControlComparisonOutput_EnabledChanged);
            // 
            // MainForm
            // 
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.None;
            this.ClientSize = new System.Drawing.Size(567, 376);
            this.Controls.Add(this._userControlConnect);
            this.Controls.Add(this._userControlProgress);
            this.Controls.Add(this._userControlComparisonOutput);
            this.MaximizeBox = false;
            this.MaximumSize = new System.Drawing.Size(583, 415);
            this.MinimumSize = new System.Drawing.Size(583, 415);
            this.Name = "MainForm";
            this.Text = "Database comparison form";
            this.ResumeLayout(false);

        }

        #endregion

        private UserControls.UserControlConnect _userControlConnect;
        private UserControls.UserControlProgress _userControlProgress;
        private UserControls.UserControlComparisonOutput _userControlComparisonOutput;
    }
}

namespace DataReader
{
    partial class Form1
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
            this.btnStart = new System.Windows.Forms.Button();
            this.btnStop = new System.Windows.Forms.Button();
            this.lbData = new System.Windows.Forms.ListBox();
            this.Progress = new System.Windows.Forms.ProgressBar();
            this.BackgroundWorkerReader = new System.ComponentModel.BackgroundWorker();
            this.btnReset = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // btnStart
            // 
            this.btnStart.Location = new System.Drawing.Point(12, 12);
            this.btnStart.Name = "btnStart";
            this.btnStart.Size = new System.Drawing.Size(75, 23);
            this.btnStart.TabIndex = 0;
            this.btnStart.Text = "Start reading";
            this.btnStart.UseVisualStyleBackColor = true;
            this.btnStart.Click += new System.EventHandler(this.btnStart_Click);
            // 
            // btnStop
            // 
            this.btnStop.Enabled = false;
            this.btnStop.Location = new System.Drawing.Point(197, 12);
            this.btnStop.Name = "btnStop";
            this.btnStop.Size = new System.Drawing.Size(75, 23);
            this.btnStop.TabIndex = 1;
            this.btnStop.Text = "Abort";
            this.btnStop.UseVisualStyleBackColor = true;
            this.btnStop.Click += new System.EventHandler(this.btnStop_Click);
            // 
            // lbData
            // 
            this.lbData.FormattingEnabled = true;
            this.lbData.Location = new System.Drawing.Point(12, 41);
            this.lbData.Name = "lbData";
            this.lbData.Size = new System.Drawing.Size(260, 186);
            this.lbData.TabIndex = 2;
            // 
            // Progress
            // 
            this.Progress.Location = new System.Drawing.Point(12, 233);
            this.Progress.Name = "Progress";
            this.Progress.Size = new System.Drawing.Size(260, 23);
            this.Progress.TabIndex = 3;
            // 
            // BackgroundWorkerReader
            // 
            this.BackgroundWorkerReader.WorkerReportsProgress = true;
            this.BackgroundWorkerReader.WorkerSupportsCancellation = true;
            this.BackgroundWorkerReader.DoWork += new System.ComponentModel.DoWorkEventHandler(this.BackgroundWorkerReader_DoWork);
            this.BackgroundWorkerReader.ProgressChanged += new System.ComponentModel.ProgressChangedEventHandler(this.BackgroundWorkerReader_ProgressChanged);
            this.BackgroundWorkerReader.RunWorkerCompleted += new System.ComponentModel.RunWorkerCompletedEventHandler(this.BackgroundWorkerReader_RunWorkerCompleted);
            // 
            // btnReset
            // 
            this.btnReset.Location = new System.Drawing.Point(93, 12);
            this.btnReset.Name = "btnReset";
            this.btnReset.Size = new System.Drawing.Size(98, 23);
            this.btnReset.TabIndex = 4;
            this.btnReset.Text = "Reset";
            this.btnReset.UseVisualStyleBackColor = true;
            this.btnReset.Click += new System.EventHandler(this.btnReset_Click);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(284, 268);
            this.Controls.Add(this.btnReset);
            this.Controls.Add(this.Progress);
            this.Controls.Add(this.lbData);
            this.Controls.Add(this.btnStop);
            this.Controls.Add(this.btnStart);
            this.Name = "Form1";
            this.Text = "DataReader";
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Button btnStart;
        private System.Windows.Forms.Button btnStop;
        private System.Windows.Forms.ListBox lbData;
        private System.Windows.Forms.ProgressBar Progress;
        private System.ComponentModel.BackgroundWorker BackgroundWorkerReader;
        private System.Windows.Forms.Button btnReset;
    }
}

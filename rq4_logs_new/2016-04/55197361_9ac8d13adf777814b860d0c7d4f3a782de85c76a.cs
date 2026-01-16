namespace Restaurant
{
    partial class EmployeeActionSelector
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
            this.btnManager = new System.Windows.Forms.Button();
            this.btnWaiter = new System.Windows.Forms.Button();
            this.btnCook = new System.Windows.Forms.Button();
            this.btnBusser = new System.Windows.Forms.Button();
            this.btnHost = new System.Windows.Forms.Button();
            this.btnClockIn = new System.Windows.Forms.Button();
            this.btnClockOut = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // btnManager
            // 
            this.btnManager.Location = new System.Drawing.Point(83, 31);
            this.btnManager.Name = "btnManager";
            this.btnManager.Size = new System.Drawing.Size(75, 23);
            this.btnManager.TabIndex = 0;
            this.btnManager.Text = "Manager";
            this.btnManager.UseVisualStyleBackColor = true;
            this.btnManager.Click += new System.EventHandler(this.btnManager_Click);
            // 
            // btnWaiter
            // 
            this.btnWaiter.Location = new System.Drawing.Point(83, 60);
            this.btnWaiter.Name = "btnWaiter";
            this.btnWaiter.Size = new System.Drawing.Size(75, 23);
            this.btnWaiter.TabIndex = 1;
            this.btnWaiter.Text = "Waiter";
            this.btnWaiter.UseVisualStyleBackColor = true;
            this.btnWaiter.Click += new System.EventHandler(this.btnWaiter_Click);
            // 
            // btnCook
            // 
            this.btnCook.Location = new System.Drawing.Point(83, 89);
            this.btnCook.Name = "btnCook";
            this.btnCook.Size = new System.Drawing.Size(75, 23);
            this.btnCook.TabIndex = 2;
            this.btnCook.Text = "Cook";
            this.btnCook.UseVisualStyleBackColor = true;
            this.btnCook.Click += new System.EventHandler(this.btnCook_Click);
            // 
            // btnBusser
            // 
            this.btnBusser.Location = new System.Drawing.Point(83, 118);
            this.btnBusser.Name = "btnBusser";
            this.btnBusser.Size = new System.Drawing.Size(75, 23);
            this.btnBusser.TabIndex = 3;
            this.btnBusser.Text = "Busser";
            this.btnBusser.UseVisualStyleBackColor = true;
            this.btnBusser.Click += new System.EventHandler(this.btnBusser_Click);
            // 
            // btnHost
            // 
            this.btnHost.Location = new System.Drawing.Point(83, 147);
            this.btnHost.Name = "btnHost";
            this.btnHost.Size = new System.Drawing.Size(75, 23);
            this.btnHost.TabIndex = 4;
            this.btnHost.Text = "Host";
            this.btnHost.UseVisualStyleBackColor = true;
            this.btnHost.Click += new System.EventHandler(this.btnHost_Click);
            // 
            // btnClockIn
            // 
            this.btnClockIn.Location = new System.Drawing.Point(27, 217);
            this.btnClockIn.Name = "btnClockIn";
            this.btnClockIn.Size = new System.Drawing.Size(75, 23);
            this.btnClockIn.TabIndex = 5;
            this.btnClockIn.Text = "Clock In";
            this.btnClockIn.UseVisualStyleBackColor = true;
            // 
            // btnClockOut
            // 
            this.btnClockOut.Location = new System.Drawing.Point(197, 217);
            this.btnClockOut.Name = "btnClockOut";
            this.btnClockOut.Size = new System.Drawing.Size(75, 23);
            this.btnClockOut.TabIndex = 6;
            this.btnClockOut.Text = "Clock Out";
            this.btnClockOut.UseVisualStyleBackColor = true;
            // 
            // EmployeeActionSelector
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(284, 262);
            this.Controls.Add(this.btnClockOut);
            this.Controls.Add(this.btnClockIn);
            this.Controls.Add(this.btnHost);
            this.Controls.Add(this.btnBusser);
            this.Controls.Add(this.btnCook);
            this.Controls.Add(this.btnWaiter);
            this.Controls.Add(this.btnManager);
            this.Name = "EmployeeActionSelector";
            this.Text = "EmployeeActionSelector";
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Button btnManager;
        private System.Windows.Forms.Button btnWaiter;
        private System.Windows.Forms.Button btnCook;
        private System.Windows.Forms.Button btnBusser;
        private System.Windows.Forms.Button btnHost;
        private System.Windows.Forms.Button btnClockIn;
        private System.Windows.Forms.Button btnClockOut;
    }
}
namespace JobFairApp.Forms
{
    partial class CreateInterviewTimes
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
            this.lblTest = new System.Windows.Forms.Label();
            this.lblTimes = new System.Windows.Forms.Label();
            this.tbEndTime = new System.Windows.Forms.TextBox();
            this.tbStartTime = new System.Windows.Forms.TextBox();
            this.lblDash2 = new System.Windows.Forms.Label();
            this.cbBreak = new System.Windows.Forms.CheckBox();
            this.tbBreakEnd = new System.Windows.Forms.TextBox();
            this.tbBreakStart = new System.Windows.Forms.TextBox();
            this.lblDash = new System.Windows.Forms.Label();
            this.btnSubmit = new System.Windows.Forms.Button();
            this.gbInterviewLengths = new System.Windows.Forms.GroupBox();
            this.rb20Mins = new System.Windows.Forms.RadioButton();
            this.rb30Mins = new System.Windows.Forms.RadioButton();
            this.rb15Mins = new System.Windows.Forms.RadioButton();
            this.gbInterviewLengths.SuspendLayout();
            this.SuspendLayout();
            // 
            // lblTest
            // 
            this.lblTest.AutoSize = true;
            this.lblTest.Location = new System.Drawing.Point(-27, 12);
            this.lblTest.Name = "lblTest";
            this.lblTest.Size = new System.Drawing.Size(176, 13);
            this.lblTest.TabIndex = 26;
            this.lblTest.Text = "Placeholder for getting the job fair id";
            // 
            // lblTimes
            // 
            this.lblTimes.AutoSize = true;
            this.lblTimes.Location = new System.Drawing.Point(12, 115);
            this.lblTimes.Name = "lblTimes";
            this.lblTimes.Size = new System.Drawing.Size(269, 13);
            this.lblTimes.TabIndex = 25;
            this.lblTimes.Text = "Start and End Times for Interviewing (Format: hh:mm:ss)";
            // 
            // tbEndTime
            // 
            this.tbEndTime.Location = new System.Drawing.Point(148, 141);
            this.tbEndTime.Name = "tbEndTime";
            this.tbEndTime.Size = new System.Drawing.Size(100, 20);
            this.tbEndTime.TabIndex = 24;
            // 
            // tbStartTime
            // 
            this.tbStartTime.Location = new System.Drawing.Point(12, 141);
            this.tbStartTime.Name = "tbStartTime";
            this.tbStartTime.Size = new System.Drawing.Size(100, 20);
            this.tbStartTime.TabIndex = 23;
            // 
            // lblDash2
            // 
            this.lblDash2.AutoSize = true;
            this.lblDash2.Font = new System.Drawing.Font("Microsoft Sans Serif", 20.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblDash2.Location = new System.Drawing.Point(118, 133);
            this.lblDash2.Name = "lblDash2";
            this.lblDash2.Size = new System.Drawing.Size(24, 31);
            this.lblDash2.TabIndex = 22;
            this.lblDash2.Text = "-";
            // 
            // cbBreak
            // 
            this.cbBreak.AutoSize = true;
            this.cbBreak.Location = new System.Drawing.Point(12, 177);
            this.cbBreak.Name = "cbBreak";
            this.cbBreak.Size = new System.Drawing.Size(137, 17);
            this.cbBreak.TabIndex = 21;
            this.cbBreak.Text = "Insert A Break? (Below)";
            this.cbBreak.UseVisualStyleBackColor = true;
            // 
            // tbBreakEnd
            // 
            this.tbBreakEnd.Location = new System.Drawing.Point(148, 200);
            this.tbBreakEnd.Name = "tbBreakEnd";
            this.tbBreakEnd.Size = new System.Drawing.Size(100, 20);
            this.tbBreakEnd.TabIndex = 20;
            // 
            // tbBreakStart
            // 
            this.tbBreakStart.Location = new System.Drawing.Point(12, 200);
            this.tbBreakStart.Name = "tbBreakStart";
            this.tbBreakStart.Size = new System.Drawing.Size(100, 20);
            this.tbBreakStart.TabIndex = 19;
            // 
            // lblDash
            // 
            this.lblDash.AutoSize = true;
            this.lblDash.Font = new System.Drawing.Font("Microsoft Sans Serif", 20.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblDash.Location = new System.Drawing.Point(118, 192);
            this.lblDash.Name = "lblDash";
            this.lblDash.Size = new System.Drawing.Size(24, 31);
            this.lblDash.TabIndex = 18;
            this.lblDash.Text = "-";
            // 
            // btnSubmit
            // 
            this.btnSubmit.Location = new System.Drawing.Point(196, 226);
            this.btnSubmit.Name = "btnSubmit";
            this.btnSubmit.Size = new System.Drawing.Size(75, 23);
            this.btnSubmit.TabIndex = 17;
            this.btnSubmit.Text = "Submit";
            this.btnSubmit.UseVisualStyleBackColor = true;
            this.btnSubmit.Click += new System.EventHandler(this.btnSubmit_Click);
            // 
            // gbInterviewLengths
            // 
            this.gbInterviewLengths.Controls.Add(this.rb20Mins);
            this.gbInterviewLengths.Controls.Add(this.rb30Mins);
            this.gbInterviewLengths.Controls.Add(this.rb15Mins);
            this.gbInterviewLengths.Location = new System.Drawing.Point(154, 12);
            this.gbInterviewLengths.Name = "gbInterviewLengths";
            this.gbInterviewLengths.Size = new System.Drawing.Size(118, 100);
            this.gbInterviewLengths.TabIndex = 16;
            this.gbInterviewLengths.TabStop = false;
            this.gbInterviewLengths.Text = "Interview Lengths";
            // 
            // rb20Mins
            // 
            this.rb20Mins.AutoSize = true;
            this.rb20Mins.Location = new System.Drawing.Point(6, 45);
            this.rb20Mins.Name = "rb20Mins";
            this.rb20Mins.Size = new System.Drawing.Size(77, 17);
            this.rb20Mins.TabIndex = 3;
            this.rb20Mins.TabStop = true;
            this.rb20Mins.Text = "20 Minutes";
            this.rb20Mins.UseVisualStyleBackColor = true;
            // 
            // rb30Mins
            // 
            this.rb30Mins.AutoSize = true;
            this.rb30Mins.Location = new System.Drawing.Point(6, 69);
            this.rb30Mins.Name = "rb30Mins";
            this.rb30Mins.Size = new System.Drawing.Size(77, 17);
            this.rb30Mins.TabIndex = 4;
            this.rb30Mins.TabStop = true;
            this.rb30Mins.Text = "30 Minutes";
            this.rb30Mins.UseVisualStyleBackColor = true;
            // 
            // rb15Mins
            // 
            this.rb15Mins.AutoSize = true;
            this.rb15Mins.Location = new System.Drawing.Point(6, 21);
            this.rb15Mins.Name = "rb15Mins";
            this.rb15Mins.Size = new System.Drawing.Size(77, 17);
            this.rb15Mins.TabIndex = 2;
            this.rb15Mins.TabStop = true;
            this.rb15Mins.Text = "15 Minutes";
            this.rb15Mins.UseVisualStyleBackColor = true;
            // 
            // CreateInterviewTimes
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(284, 261);
            this.Controls.Add(this.lblTest);
            this.Controls.Add(this.lblTimes);
            this.Controls.Add(this.tbEndTime);
            this.Controls.Add(this.tbStartTime);
            this.Controls.Add(this.lblDash2);
            this.Controls.Add(this.cbBreak);
            this.Controls.Add(this.tbBreakEnd);
            this.Controls.Add(this.tbBreakStart);
            this.Controls.Add(this.lblDash);
            this.Controls.Add(this.btnSubmit);
            this.Controls.Add(this.gbInterviewLengths);
            this.Name = "CreateInterviewTimes";
            this.Text = "CreateInterviewTimes";
            this.gbInterviewLengths.ResumeLayout(false);
            this.gbInterviewLengths.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label lblTest;
        private System.Windows.Forms.Label lblTimes;
        private System.Windows.Forms.TextBox tbEndTime;
        private System.Windows.Forms.TextBox tbStartTime;
        private System.Windows.Forms.Label lblDash2;
        private System.Windows.Forms.CheckBox cbBreak;
        private System.Windows.Forms.TextBox tbBreakEnd;
        private System.Windows.Forms.TextBox tbBreakStart;
        private System.Windows.Forms.Label lblDash;
        private System.Windows.Forms.Button btnSubmit;
        private System.Windows.Forms.GroupBox gbInterviewLengths;
        private System.Windows.Forms.RadioButton rb20Mins;
        private System.Windows.Forms.RadioButton rb30Mins;
        private System.Windows.Forms.RadioButton rb15Mins;
    }
}
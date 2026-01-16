namespace MAS_System_
{
    partial class Threshold
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
            this.tempThreshNewLbl = new System.Windows.Forms.Label();
            this.threshSmokeNewLbl = new System.Windows.Forms.Label();
            this.humThreshNewlbl = new System.Windows.Forms.Label();
            this.tempNewThreshTxt = new System.Windows.Forms.TextBox();
            this.smokeThreshNewBtn = new System.Windows.Forms.TextBox();
            this.humThreshNewBtn = new System.Windows.Forms.TextBox();
            this.setThreshBtn = new System.Windows.Forms.Button();
            this.cancelThreshBtn = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // tempThreshNewLbl
            // 
            this.tempThreshNewLbl.AutoSize = true;
            this.tempThreshNewLbl.Location = new System.Drawing.Point(106, 186);
            this.tempThreshNewLbl.Name = "tempThreshNewLbl";
            this.tempThreshNewLbl.Size = new System.Drawing.Size(200, 37);
            this.tempThreshNewLbl.TabIndex = 0;
            this.tempThreshNewLbl.Text = "Temperature";
            // 
            // threshSmokeNewLbl
            // 
            this.threshSmokeNewLbl.AutoSize = true;
            this.threshSmokeNewLbl.Location = new System.Drawing.Point(394, 186);
            this.threshSmokeNewLbl.Name = "threshSmokeNewLbl";
            this.threshSmokeNewLbl.Size = new System.Drawing.Size(116, 37);
            this.threshSmokeNewLbl.TabIndex = 1;
            this.threshSmokeNewLbl.Text = "Smoke";
            // 
            // humThreshNewlbl
            // 
            this.humThreshNewlbl.AutoSize = true;
            this.humThreshNewlbl.Location = new System.Drawing.Point(714, 186);
            this.humThreshNewlbl.Name = "humThreshNewlbl";
            this.humThreshNewlbl.Size = new System.Drawing.Size(141, 37);
            this.humThreshNewlbl.TabIndex = 2;
            this.humThreshNewlbl.Text = "Humidity";
            // 
            // tempNewThreshTxt
            // 
            this.tempNewThreshTxt.Location = new System.Drawing.Point(113, 279);
            this.tempNewThreshTxt.Name = "tempNewThreshTxt";
            this.tempNewThreshTxt.Size = new System.Drawing.Size(223, 44);
            this.tempNewThreshTxt.TabIndex = 3;
            // 
            // smokeThreshNewBtn
            // 
            this.smokeThreshNewBtn.Location = new System.Drawing.Point(401, 279);
            this.smokeThreshNewBtn.Name = "smokeThreshNewBtn";
            this.smokeThreshNewBtn.Size = new System.Drawing.Size(223, 44);
            this.smokeThreshNewBtn.TabIndex = 4;
            // 
            // humThreshNewBtn
            // 
            this.humThreshNewBtn.Location = new System.Drawing.Point(707, 279);
            this.humThreshNewBtn.Name = "humThreshNewBtn";
            this.humThreshNewBtn.Size = new System.Drawing.Size(223, 44);
            this.humThreshNewBtn.TabIndex = 5;
            // 
            // setThreshBtn
            // 
            this.setThreshBtn.Location = new System.Drawing.Point(130, 585);
            this.setThreshBtn.Name = "setThreshBtn";
            this.setThreshBtn.Size = new System.Drawing.Size(194, 68);
            this.setThreshBtn.TabIndex = 6;
            this.setThreshBtn.Text = "Set";
            this.setThreshBtn.UseVisualStyleBackColor = true;
            this.setThreshBtn.Click += new System.EventHandler(this.setThreshBtn_Click);
            // 
            // cancelThreshBtn
            // 
            this.cancelThreshBtn.Location = new System.Drawing.Point(677, 585);
            this.cancelThreshBtn.Name = "cancelThreshBtn";
            this.cancelThreshBtn.Size = new System.Drawing.Size(194, 68);
            this.cancelThreshBtn.TabIndex = 7;
            this.cancelThreshBtn.Text = "Cancel";
            this.cancelThreshBtn.UseVisualStyleBackColor = true;
            this.cancelThreshBtn.Click += new System.EventHandler(this.cancelThreshBtn_Click);
            // 
            // Threshold
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(19F, 37F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1003, 767);
            this.Controls.Add(this.cancelThreshBtn);
            this.Controls.Add(this.setThreshBtn);
            this.Controls.Add(this.humThreshNewBtn);
            this.Controls.Add(this.smokeThreshNewBtn);
            this.Controls.Add(this.tempNewThreshTxt);
            this.Controls.Add(this.humThreshNewlbl);
            this.Controls.Add(this.threshSmokeNewLbl);
            this.Controls.Add(this.tempThreshNewLbl);
            this.Name = "Threshold";
            this.Text = "Threshold";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label tempThreshNewLbl;
        private System.Windows.Forms.Label threshSmokeNewLbl;
        private System.Windows.Forms.Label humThreshNewlbl;
        private System.Windows.Forms.TextBox tempNewThreshTxt;
        private System.Windows.Forms.TextBox smokeThreshNewBtn;
        private System.Windows.Forms.TextBox humThreshNewBtn;
        private System.Windows.Forms.Button setThreshBtn;
        private System.Windows.Forms.Button cancelThreshBtn;
    }
}
namespace Triangles
{
    partial class TriangleForm
    {
        /// <summary>
        ///  Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        ///  Clean up any resources being used.
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
        ///  Required method for Designer support - do not modify
        ///  the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.label1 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.OutputLabel = new System.Windows.Forms.Label();
            this.SideLength_a = new System.Windows.Forms.TextBox();
            this.SideLength_b = new System.Windows.Forms.TextBox();
            this.SideLength_c = new System.Windows.Forms.TextBox();
            this.AngleMeasurementsLabel = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(12, 15);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(78, 15);
            this.label1.TabIndex = 3;
            this.label1.Text = "Side a length:";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(11, 44);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(79, 15);
            this.label2.TabIndex = 4;
            this.label2.Text = "Side b length:";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(12, 73);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(78, 15);
            this.label3.TabIndex = 5;
            this.label3.Text = "Side c length:";
            // 
            // OutputLabel
            // 
            this.OutputLabel.AutoSize = true;
            this.OutputLabel.Location = new System.Drawing.Point(12, 113);
            this.OutputLabel.Name = "OutputLabel";
            this.OutputLabel.Size = new System.Drawing.Size(100, 15);
            this.OutputLabel.TabIndex = 6;
            this.OutputLabel.Text = "Enter side lengths";
            // 
            // SideLength_a
            // 
            this.SideLength_a.Location = new System.Drawing.Point(96, 12);
            this.SideLength_a.Name = "SideLength_a";
            this.SideLength_a.Size = new System.Drawing.Size(100, 23);
            this.SideLength_a.TabIndex = 7;
            // 
            // SideLength_b
            // 
            this.SideLength_b.Location = new System.Drawing.Point(96, 41);
            this.SideLength_b.Name = "SideLength_b";
            this.SideLength_b.Size = new System.Drawing.Size(100, 23);
            this.SideLength_b.TabIndex = 8;
            // 
            // SideLength_c
            // 
            this.SideLength_c.Location = new System.Drawing.Point(96, 70);
            this.SideLength_c.Name = "SideLength_c";
            this.SideLength_c.Size = new System.Drawing.Size(100, 23);
            this.SideLength_c.TabIndex = 9;
            // 
            // AngleMeasurementsLabel
            // 
            this.AngleMeasurementsLabel.AutoSize = true;
            this.AngleMeasurementsLabel.Location = new System.Drawing.Point(12, 137);
            this.AngleMeasurementsLabel.Name = "AngleMeasurementsLabel";
            this.AngleMeasurementsLabel.Size = new System.Drawing.Size(0, 15);
            this.AngleMeasurementsLabel.TabIndex = 10;
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(7F, 15F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(258, 221);
            this.Controls.Add(this.AngleMeasurementsLabel);
            this.Controls.Add(this.SideLength_c);
            this.Controls.Add(this.SideLength_b);
            this.Controls.Add(this.SideLength_a);
            this.Controls.Add(this.OutputLabel);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.label1);
            this.Name = "Form1";
            this.Text = "Form1";
            this.Load += new System.EventHandler(this.Form1_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private Label label1;
        private Label label2;
        private Label label3;
        private Label OutputLabel;
        private TextBox SideLength_a;
        private TextBox SideLength_b;
        private TextBox SideLength_c;
        private Label AngleMeasurementsLabel;
    }
}
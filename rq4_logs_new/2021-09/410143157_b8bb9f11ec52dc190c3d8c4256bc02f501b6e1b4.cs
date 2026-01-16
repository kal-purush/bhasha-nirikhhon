
namespace BinaryRepresentor
{
    partial class Form1
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
            this.groupBox_input = new System.Windows.Forms.GroupBox();
            this.groupBox_result = new System.Windows.Forms.GroupBox();
            this.button_compute = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // groupBox_input
            // 
            this.groupBox_input.Location = new System.Drawing.Point(12, 12);
            this.groupBox_input.Name = "groupBox_input";
            this.groupBox_input.Size = new System.Drawing.Size(701, 120);
            this.groupBox_input.TabIndex = 0;
            this.groupBox_input.TabStop = false;
            this.groupBox_input.Text = "Input";
            // 
            // groupBox_result
            // 
            this.groupBox_result.Location = new System.Drawing.Point(12, 202);
            this.groupBox_result.Name = "groupBox_result";
            this.groupBox_result.Size = new System.Drawing.Size(701, 103);
            this.groupBox_result.TabIndex = 1;
            this.groupBox_result.TabStop = false;
            this.groupBox_result.Text = "Result";
            // 
            // button_compute
            // 
            this.button_compute.Location = new System.Drawing.Point(314, 153);
            this.button_compute.Name = "button_compute";
            this.button_compute.Size = new System.Drawing.Size(150, 43);
            this.button_compute.TabIndex = 2;
            this.button_compute.Text = "Compute";
            this.button_compute.UseVisualStyleBackColor = true;
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(10F, 25F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(730, 317);
            this.Controls.Add(this.button_compute);
            this.Controls.Add(this.groupBox_result);
            this.Controls.Add(this.groupBox_input);
            this.Name = "Form1";
            this.Text = "Binary Representor";
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.GroupBox groupBox_input;
        private System.Windows.Forms.GroupBox groupBox_result;
        private System.Windows.Forms.Button button_compute;
    }
}

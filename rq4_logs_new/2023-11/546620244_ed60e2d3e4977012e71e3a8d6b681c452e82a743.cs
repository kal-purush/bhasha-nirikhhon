namespace Application11 {
    partial class Form1 {
        /// <summary>
        ///  Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        ///  Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing) {
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
        private void InitializeComponent() {
            num_samples = new NumericUpDown();
            samples_size = new NumericUpDown();
            paramsGroupBox = new GroupBox();
            switch_distrib = new Button();
            sample_button = new Button();
            num_lambda = new NumericUpDown();
            label3 = new Label();
            label2 = new Label();
            label1 = new Label();
            picturebox = new PictureBox();
            ((System.ComponentModel.ISupportInitialize)num_samples).BeginInit();
            ((System.ComponentModel.ISupportInitialize)samples_size).BeginInit();
            paramsGroupBox.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)num_lambda).BeginInit();
            ((System.ComponentModel.ISupportInitialize)picturebox).BeginInit();
            SuspendLayout();
            // 
            // num_samples
            // 
            num_samples.Location = new Point(148, 22);
            num_samples.Maximum = new decimal(new int[] { 1000, 0, 0, 0 });
            num_samples.Minimum = new decimal(new int[] { 100, 0, 0, 0 });
            num_samples.Name = "num_samples";
            num_samples.Size = new Size(90, 23);
            num_samples.TabIndex = 0;
            num_samples.Value = new decimal(new int[] { 100, 0, 0, 0 });
            // 
            // samples_size
            // 
            samples_size.Location = new Point(370, 22);
            samples_size.Minimum = new decimal(new int[] { 10, 0, 0, 0 });
            samples_size.Name = "samples_size";
            samples_size.Size = new Size(90, 23);
            samples_size.TabIndex = 1;
            samples_size.Value = new decimal(new int[] { 10, 0, 0, 0 });
            // 
            // paramsGroupBox
            // 
            paramsGroupBox.Controls.Add(switch_distrib);
            paramsGroupBox.Controls.Add(sample_button);
            paramsGroupBox.Controls.Add(num_lambda);
            paramsGroupBox.Controls.Add(label3);
            paramsGroupBox.Controls.Add(label2);
            paramsGroupBox.Controls.Add(label1);
            paramsGroupBox.Controls.Add(samples_size);
            paramsGroupBox.Controls.Add(num_samples);
            paramsGroupBox.Location = new Point(12, 12);
            paramsGroupBox.Name = "paramsGroupBox";
            paramsGroupBox.Size = new Size(975, 66);
            paramsGroupBox.TabIndex = 2;
            paramsGroupBox.TabStop = false;
            paramsGroupBox.Text = "Application Parameters";
            // 
            // switch_distrib
            // 
            switch_distrib.Location = new Point(811, 20);
            switch_distrib.Name = "switch_distrib";
            switch_distrib.Size = new Size(147, 26);
            switch_distrib.TabIndex = 6;
            switch_distrib.Text = "Show Interarrivals";
            switch_distrib.UseVisualStyleBackColor = true;
            switch_distrib.Click += switch_distrib_Click;
            // 
            // sample_button
            // 
            sample_button.Location = new Point(675, 17);
            sample_button.Name = "sample_button";
            sample_button.Size = new Size(130, 29);
            sample_button.TabIndex = 4;
            sample_button.Text = "Generate Samples";
            sample_button.UseVisualStyleBackColor = true;
            sample_button.Click += sample_button_Click;
            // 
            // num_lambda
            // 
            num_lambda.Location = new Point(545, 22);
            num_lambda.Name = "num_lambda";
            num_lambda.Size = new Size(90, 23);
            num_lambda.TabIndex = 5;
            num_lambda.Value = new decimal(new int[] { 10, 0, 0, 0 });
            // 
            // label3
            // 
            label3.AutoSize = true;
            label3.Location = new Point(489, 24);
            label3.Name = "label3";
            label3.Size = new Size(50, 15);
            label3.TabIndex = 4;
            label3.Text = "lambda:";
            // 
            // label2
            // 
            label2.AutoSize = true;
            label2.Location = new Point(268, 24);
            label2.Name = "label2";
            label2.Size = new Size(96, 15);
            label2.TabIndex = 3;
            label2.Text = "Samples size (N):";
            // 
            // label1
            // 
            label1.AutoSize = true;
            label1.Location = new Point(6, 24);
            label1.Name = "label1";
            label1.Size = new Size(136, 15);
            label1.TabIndex = 2;
            label1.Text = "Number of samples (M):";
            // 
            // picturebox
            // 
            picturebox.Location = new Point(12, 84);
            picturebox.Name = "picturebox";
            picturebox.Size = new Size(975, 503);
            picturebox.TabIndex = 3;
            picturebox.TabStop = false;
            // 
            // Form1
            // 
            AutoScaleDimensions = new SizeF(7F, 15F);
            AutoScaleMode = AutoScaleMode.Font;
            ClientSize = new Size(994, 599);
            Controls.Add(picturebox);
            Controls.Add(paramsGroupBox);
            Name = "Form1";
            Text = "Form1";
            Load += Form1_Load;
            ((System.ComponentModel.ISupportInitialize)num_samples).EndInit();
            ((System.ComponentModel.ISupportInitialize)samples_size).EndInit();
            paramsGroupBox.ResumeLayout(false);
            paramsGroupBox.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)num_lambda).EndInit();
            ((System.ComponentModel.ISupportInitialize)picturebox).EndInit();
            ResumeLayout(false);
        }

        #endregion

        private NumericUpDown num_samples;
        private NumericUpDown samples_size;
        private GroupBox paramsGroupBox;
        private Label label2;
        private Label label1;
        private PictureBox picturebox;
        private Button sample_button;
        private Label label3;
        private NumericUpDown num_lambda;
        private Button switch_distrib;
    }
}
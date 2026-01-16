namespace CrashGraphs
{
    partial class CrashGraph2
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
            this.label3 = new System.Windows.Forms.Label();
            this.txtTest = new System.Windows.Forms.TextBox();
            this.label2 = new System.Windows.Forms.Label();
            this.txtDest = new System.Windows.Forms.TextBox();
            this.label1 = new System.Windows.Forms.Label();
            this.txtTrain = new System.Windows.Forms.TextBox();
            this.button2 = new System.Windows.Forms.Button();
            this.button1 = new System.Windows.Forms.Button();
            this.btnCrashGraph = new System.Windows.Forms.Button();
            this.button3 = new System.Windows.Forms.Button();
            this.button4 = new System.Windows.Forms.Button();
            this.label10 = new System.Windows.Forms.Label();
            this.label9 = new System.Windows.Forms.Label();
            this.txtNTraces = new System.Windows.Forms.TextBox();
            this.txtNBuckets = new System.Windows.Forms.TextBox();
            this.label4 = new System.Windows.Forms.Label();
            this.label5 = new System.Windows.Forms.Label();
            this.button5 = new System.Windows.Forms.Button();
            this.txtResult = new System.Windows.Forms.TextBox();
            this.label6 = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(12, 74);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(65, 13);
            this.label3.TabIndex = 17;
            this.label3.Text = "Source Test";
            // 
            // txtTest
            // 
            this.txtTest.Location = new System.Drawing.Point(86, 71);
            this.txtTest.Name = "txtTest";
            this.txtTest.Size = new System.Drawing.Size(330, 20);
            this.txtTest.TabIndex = 16;
            this.txtTest.Text = "Test set path";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(12, 100);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(60, 13);
            this.label2.TabIndex = 15;
            this.label2.Text = "Destination";
            // 
            // txtDest
            // 
            this.txtDest.Location = new System.Drawing.Point(86, 97);
            this.txtDest.Name = "txtDest";
            this.txtDest.Size = new System.Drawing.Size(330, 20);
            this.txtDest.TabIndex = 14;
            this.txtDest.Text = "Detination saving path";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(12, 49);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(68, 13);
            this.label1.TabIndex = 13;
            this.label1.Text = "Source Train";
            // 
            // txtTrain
            // 
            this.txtTrain.Location = new System.Drawing.Point(86, 46);
            this.txtTrain.Name = "txtTrain";
            this.txtTrain.Size = new System.Drawing.Size(330, 20);
            this.txtTrain.TabIndex = 12;
            this.txtTrain.Text = "Training path";
            this.txtTrain.TextChanged += new System.EventHandler(this.txtTrain_TextChanged);
            // 
            // button2
            // 
            this.button2.Location = new System.Drawing.Point(132, 239);
            this.button2.Name = "button2";
            this.button2.Size = new System.Drawing.Size(144, 23);
            this.button2.TabIndex = 11;
            this.button2.Text = "Data to Table";
            this.button2.UseVisualStyleBackColor = true;
            this.button2.Click += new System.EventHandler(this.button2_Click);
            // 
            // button1
            // 
            this.button1.Location = new System.Drawing.Point(150, 141);
            this.button1.Name = "button1";
            this.button1.Size = new System.Drawing.Size(99, 23);
            this.button1.TabIndex = 10;
            this.button1.Text = "Crash Graphs";
            this.button1.UseVisualStyleBackColor = true;
            this.button1.Click += new System.EventHandler(this.button1_Click);
            // 
            // btnCrashGraph
            // 
            this.btnCrashGraph.Location = new System.Drawing.Point(132, 189);
            this.btnCrashGraph.Name = "btnCrashGraph";
            this.btnCrashGraph.Size = new System.Drawing.Size(144, 23);
            this.btnCrashGraph.TabIndex = 9;
            this.btnCrashGraph.Text = "Making Results";
            this.btnCrashGraph.UseVisualStyleBackColor = true;
            this.btnCrashGraph.Click += new System.EventHandler(this.btnCrashGraph_Click);
            // 
            // button3
            // 
            this.button3.Location = new System.Drawing.Point(105, 287);
            this.button3.Name = "button3";
            this.button3.Size = new System.Drawing.Size(199, 23);
            this.button3.TabIndex = 18;
            this.button3.Text = "Results with scores";
            this.button3.UseVisualStyleBackColor = true;
            this.button3.Click += new System.EventHandler(this.button3_Click_1);
            // 
            // button4
            // 
            this.button4.Location = new System.Drawing.Point(115, 472);
            this.button4.Name = "button4";
            this.button4.Size = new System.Drawing.Size(199, 23);
            this.button4.TabIndex = 19;
            this.button4.Text = "Accuracy with rank";
            this.button4.UseVisualStyleBackColor = true;
            this.button4.Click += new System.EventHandler(this.button4_Click);
            // 
            // label10
            // 
            this.label10.AutoSize = true;
            this.label10.Location = new System.Drawing.Point(289, 399);
            this.label10.Name = "label10";
            this.label10.Size = new System.Drawing.Size(132, 13);
            this.label10.TabIndex = 61;
            this.label10.Text = "Gnome:1232 - Firefox: 784";
            // 
            // label9
            // 
            this.label9.AutoSize = true;
            this.label9.Location = new System.Drawing.Point(289, 369);
            this.label9.Name = "label9";
            this.label9.Size = new System.Drawing.Size(127, 13);
            this.label9.TabIndex = 60;
            this.label9.Text = "Gnome: 182- FIrefox; 103";
            // 
            // txtNTraces
            // 
            this.txtNTraces.Location = new System.Drawing.Point(200, 392);
            this.txtNTraces.Name = "txtNTraces";
            this.txtNTraces.Size = new System.Drawing.Size(69, 20);
            this.txtNTraces.TabIndex = 59;
            this.txtNTraces.Text = "1232";
            // 
            // txtNBuckets
            // 
            this.txtNBuckets.Location = new System.Drawing.Point(200, 366);
            this.txtNBuckets.Name = "txtNBuckets";
            this.txtNBuckets.Size = new System.Drawing.Size(69, 20);
            this.txtNBuckets.TabIndex = 58;
            this.txtNBuckets.Text = "182";
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(63, 392);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(113, 13);
            this.label4.TabIndex = 57;
            this.label4.Text = "Total number of traces";
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Location = new System.Drawing.Point(78, 366);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(97, 13);
            this.label5.TabIndex = 56;
            this.label5.Text = "Number of buckets";
            // 
            // button5
            // 
            this.button5.Location = new System.Drawing.Point(115, 421);
            this.button5.Name = "button5";
            this.button5.Size = new System.Drawing.Size(199, 23);
            this.button5.TabIndex = 62;
            this.button5.Text = "Create labels";
            this.button5.UseVisualStyleBackColor = true;
            this.button5.Click += new System.EventHandler(this.button5_Click);
            // 
            // txtResult
            // 
            this.txtResult.Location = new System.Drawing.Point(150, 340);
            this.txtResult.Name = "txtResult";
            this.txtResult.Size = new System.Drawing.Size(343, 20);
            this.txtResult.TabIndex = 63;
            this.txtResult.Text = "Result path directory";
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Location = new System.Drawing.Point(27, 343);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(85, 13);
            this.label6.TabIndex = 64;
            this.label6.Text = "Results directory";
            // 
            // CrashGraph2
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(532, 537);
            this.Controls.Add(this.label6);
            this.Controls.Add(this.txtResult);
            this.Controls.Add(this.button5);
            this.Controls.Add(this.label10);
            this.Controls.Add(this.label9);
            this.Controls.Add(this.txtNTraces);
            this.Controls.Add(this.txtNBuckets);
            this.Controls.Add(this.label4);
            this.Controls.Add(this.label5);
            this.Controls.Add(this.button4);
            this.Controls.Add(this.button3);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.txtTest);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.txtDest);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.txtTrain);
            this.Controls.Add(this.button2);
            this.Controls.Add(this.button1);
            this.Controls.Add(this.btnCrashGraph);
            this.Name = "CrashGraph2";
            this.Text = "CrashGraph2";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.TextBox txtTest;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.TextBox txtDest;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.TextBox txtTrain;
        private System.Windows.Forms.Button button2;
        private System.Windows.Forms.Button button1;
        private System.Windows.Forms.Button btnCrashGraph;
        private System.Windows.Forms.Button button3;
        private System.Windows.Forms.Button button4;
        private System.Windows.Forms.Label label10;
        private System.Windows.Forms.Label label9;
        private System.Windows.Forms.TextBox txtNTraces;
        private System.Windows.Forms.TextBox txtNBuckets;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.Button button5;
        private System.Windows.Forms.TextBox txtResult;
        private System.Windows.Forms.Label label6;
    }
}
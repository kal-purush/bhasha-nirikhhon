namespace WindowsFormsApp1
{
    partial class Form1
    {
        /// <summary>
        /// Wymagana zmienna projektanta.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Wyczyść wszystkie używane zasoby.
        /// </summary>
        /// <param name="disposing">prawda, jeżeli zarządzane zasoby powinny zostać zlikwidowane; Fałsz w przeciwnym wypadku.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Kod generowany przez Projektanta formularzy systemu Windows

        /// <summary>
        /// Metoda wymagana do obsługi projektanta — nie należy modyfikować
        /// jej zawartości w edytorze kodu.
        /// </summary>
        private void InitializeComponent()
        {
            this.tBCapacity = new System.Windows.Forms.TextBox();
            this.tBNumber = new System.Windows.Forms.TextBox();
            this.tBSeed = new System.Windows.Forms.TextBox();
            this.bSubmit = new System.Windows.Forms.Button();
            this.label1 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.listBox1 = new System.Windows.Forms.ListBox();
            this.SuspendLayout();
            // 
            // tBCapacity
            // 
            this.tBCapacity.Location = new System.Drawing.Point(12, 29);
            this.tBCapacity.Name = "tBCapacity";
            this.tBCapacity.Size = new System.Drawing.Size(132, 22);
            this.tBCapacity.TabIndex = 0;
            this.tBCapacity.TextChanged += new System.EventHandler(this.textBox1_TextChanged);
            // 
            // tBNumber
            // 
            this.tBNumber.Location = new System.Drawing.Point(12, 72);
            this.tBNumber.Name = "tBNumber";
            this.tBNumber.Size = new System.Drawing.Size(132, 22);
            this.tBNumber.TabIndex = 1;
            this.tBNumber.TextChanged += new System.EventHandler(this.textBox1_TextChanged);
            // 
            // tBSeed
            // 
            this.tBSeed.Location = new System.Drawing.Point(12, 115);
            this.tBSeed.Name = "tBSeed";
            this.tBSeed.Size = new System.Drawing.Size(132, 22);
            this.tBSeed.TabIndex = 2;
            this.tBSeed.TextChanged += new System.EventHandler(this.textBox1_TextChanged);
            // 
            // bSubmit
            // 
            this.bSubmit.Location = new System.Drawing.Point(12, 167);
            this.bSubmit.Name = "bSubmit";
            this.bSubmit.Size = new System.Drawing.Size(132, 26);
            this.bSubmit.TabIndex = 3;
            this.bSubmit.Text = "Submit";
            this.bSubmit.UseVisualStyleBackColor = true;
            this.bSubmit.Click += new System.EventHandler(this.button1_Click);
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(12, 9);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(60, 16);
            this.label1.TabIndex = 4;
            this.label1.Text = "Capacity";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(12, 54);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(104, 16);
            this.label2.TabIndex = 5;
            this.label2.Text = "Number of items";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(12, 96);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(40, 16);
            this.label3.TabIndex = 6;
            this.label3.Text = "Seed";
            // 
            // listBox1
            // 
            this.listBox1.FormattingEnabled = true;
            this.listBox1.ItemHeight = 16;
            this.listBox1.Location = new System.Drawing.Point(12, 218);
            this.listBox1.Name = "listBox1";
            this.listBox1.Size = new System.Drawing.Size(597, 260);
            this.listBox1.TabIndex = 7;
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.SystemColors.MenuHighlight;
            this.ClientSize = new System.Drawing.Size(654, 513);
            this.Controls.Add(this.listBox1);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.bSubmit);
            this.Controls.Add(this.tBSeed);
            this.Controls.Add(this.tBNumber);
            this.Controls.Add(this.tBCapacity);
            this.Name = "Form1";
            this.Text = "Problem plecakowy";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox tBCapacity;
        private System.Windows.Forms.TextBox tBNumber;
        private System.Windows.Forms.TextBox tBSeed;
        private System.Windows.Forms.Button bSubmit;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.ListBox listBox1;
    }
}

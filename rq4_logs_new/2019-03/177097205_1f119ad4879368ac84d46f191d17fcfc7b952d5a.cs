namespace TicTacToe
{
    partial class Form2
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
            this.label_numberOfPlayers = new System.Windows.Forms.Label();
            this.textBox1 = new System.Windows.Forms.TextBox();
            this.label_winningLength = new System.Windows.Forms.Label();
            this.textBox2 = new System.Windows.Forms.TextBox();
            this.label_GridSize = new System.Windows.Forms.Label();
            this.textBox3 = new System.Windows.Forms.TextBox();
            this.btn_Start = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // label_numberOfPlayers
            // 
            this.label_numberOfPlayers.AutoSize = true;
            this.label_numberOfPlayers.Location = new System.Drawing.Point(48, 35);
            this.label_numberOfPlayers.Name = "label_numberOfPlayers";
            this.label_numberOfPlayers.Size = new System.Drawing.Size(95, 13);
            this.label_numberOfPlayers.TabIndex = 0;
            this.label_numberOfPlayers.Text = "Number Of Players";
            // 
            // textBox1
            // 
            this.textBox1.Location = new System.Drawing.Point(162, 32);
            this.textBox1.Name = "textBox1";
            this.textBox1.Size = new System.Drawing.Size(100, 20);
            this.textBox1.TabIndex = 1;
            this.textBox1.Text = "2";
            // 
            // label_winningLength
            // 
            this.label_winningLength.Location = new System.Drawing.Point(51, 76);
            this.label_winningLength.Name = "label_winningLength";
            this.label_winningLength.Size = new System.Drawing.Size(95, 13);
            this.label_winningLength.TabIndex = 2;
            this.label_winningLength.Text = "Length To Win";
            // 
            // textBox2
            // 
            this.textBox2.Location = new System.Drawing.Point(162, 76);
            this.textBox2.Name = "textBox2";
            this.textBox2.Size = new System.Drawing.Size(100, 20);
            this.textBox2.TabIndex = 3;
            this.textBox2.Text = "3";
            // 
            // label_GridSize
            // 
            this.label_GridSize.Location = new System.Drawing.Point(51, 116);
            this.label_GridSize.Name = "label_GridSize";
            this.label_GridSize.Size = new System.Drawing.Size(95, 13);
            this.label_GridSize.TabIndex = 4;
            this.label_GridSize.Text = "Grid Size";
            // 
            // textBox3
            // 
            this.textBox3.Location = new System.Drawing.Point(162, 116);
            this.textBox3.Name = "textBox3";
            this.textBox3.Size = new System.Drawing.Size(100, 20);
            this.textBox3.TabIndex = 5;
            this.textBox3.Text = "8";
            // 
            // btn_Start
            // 
            this.btn_Start.Location = new System.Drawing.Point(51, 213);
            this.btn_Start.Name = "btn_Start";
            this.btn_Start.Size = new System.Drawing.Size(75, 23);
            this.btn_Start.TabIndex = 6;
            this.btn_Start.Text = "Start Game";
            this.btn_Start.UseVisualStyleBackColor = true;
            this.btn_Start.Click += new System.EventHandler(this.btn_Start_Click);
            // 
            // Form2
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(557, 275);
            this.Controls.Add(this.btn_Start);
            this.Controls.Add(this.textBox3);
            this.Controls.Add(this.label_GridSize);
            this.Controls.Add(this.textBox2);
            this.Controls.Add(this.label_winningLength);
            this.Controls.Add(this.textBox1);
            this.Controls.Add(this.label_numberOfPlayers);
            this.Name = "Form2";
            this.Text = "Form2";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label_numberOfPlayers;
        private System.Windows.Forms.TextBox textBox1;
        private System.Windows.Forms.Label label_winningLength;
        private System.Windows.Forms.TextBox textBox2;
        private System.Windows.Forms.Label label_GridSize;
        private System.Windows.Forms.TextBox textBox3;
        private System.Windows.Forms.Button btn_Start;
    }
}
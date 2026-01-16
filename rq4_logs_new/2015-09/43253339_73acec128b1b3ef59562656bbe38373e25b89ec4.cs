namespace COP4365_Project1
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
            this.components = new System.ComponentModel.Container();
            this.Letter0 = new System.Windows.Forms.Button();
            this.Letter1 = new System.Windows.Forms.Button();
            this.Letter2 = new System.Windows.Forms.Button();
            this.Letter3 = new System.Windows.Forms.Button();
            this.Letter4 = new System.Windows.Forms.Button();
            this.GO_button = new System.Windows.Forms.Button();
            this.timer = new System.Windows.Forms.Timer(this.components);
            this.words_listBox = new System.Windows.Forms.ListBox();
            this.SuspendLayout();
            // 
            // Letter0
            // 
            this.Letter0.Location = new System.Drawing.Point(100, 82);
            this.Letter0.Name = "Letter0";
            this.Letter0.Size = new System.Drawing.Size(73, 23);
            this.Letter0.TabIndex = 0;
            this.Letter0.Text = "button1";
            this.Letter0.UseVisualStyleBackColor = true;
            this.Letter0.Click += new System.EventHandler(this.Letter0_Click);
            // 
            // Letter1
            // 
            this.Letter1.Location = new System.Drawing.Point(179, 82);
            this.Letter1.Name = "Letter1";
            this.Letter1.Size = new System.Drawing.Size(75, 23);
            this.Letter1.TabIndex = 1;
            this.Letter1.Text = "button2";
            this.Letter1.UseVisualStyleBackColor = true;
            this.Letter1.Click += new System.EventHandler(this.Letter0_Click);
            // 
            // Letter2
            // 
            this.Letter2.Location = new System.Drawing.Point(260, 82);
            this.Letter2.Name = "Letter2";
            this.Letter2.Size = new System.Drawing.Size(75, 23);
            this.Letter2.TabIndex = 2;
            this.Letter2.Text = "button3";
            this.Letter2.UseVisualStyleBackColor = true;
            this.Letter2.Click += new System.EventHandler(this.Letter0_Click);
            // 
            // Letter3
            // 
            this.Letter3.Location = new System.Drawing.Point(341, 82);
            this.Letter3.Name = "Letter3";
            this.Letter3.Size = new System.Drawing.Size(75, 23);
            this.Letter3.TabIndex = 3;
            this.Letter3.Text = "button4";
            this.Letter3.UseVisualStyleBackColor = true;
            this.Letter3.Click += new System.EventHandler(this.Letter0_Click);
            // 
            // Letter4
            // 
            this.Letter4.Location = new System.Drawing.Point(422, 82);
            this.Letter4.Name = "Letter4";
            this.Letter4.Size = new System.Drawing.Size(75, 23);
            this.Letter4.TabIndex = 4;
            this.Letter4.Text = "button5";
            this.Letter4.UseVisualStyleBackColor = true;
            this.Letter4.Click += new System.EventHandler(this.Letter0_Click);
            // 
            // GO_button
            // 
            this.GO_button.Location = new System.Drawing.Point(422, 257);
            this.GO_button.Name = "GO_button";
            this.GO_button.Size = new System.Drawing.Size(75, 23);
            this.GO_button.TabIndex = 5;
            this.GO_button.Text = "GO!";
            this.GO_button.UseVisualStyleBackColor = true;
            this.GO_button.Click += new System.EventHandler(this.GO_button_Click);
            // 
            // timer
            // 
            this.timer.Interval = 5000;
            this.timer.Tick += new System.EventHandler(this.timer1_Tick);
            // 
            // words_listBox
            // 
            this.words_listBox.FormattingEnabled = true;
            this.words_listBox.Location = new System.Drawing.Point(100, 185);
            this.words_listBox.Name = "words_listBox";
            this.words_listBox.Size = new System.Drawing.Size(198, 95);
            this.words_listBox.TabIndex = 6;
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(570, 352);
            this.Controls.Add(this.words_listBox);
            this.Controls.Add(this.GO_button);
            this.Controls.Add(this.Letter4);
            this.Controls.Add(this.Letter3);
            this.Controls.Add(this.Letter2);
            this.Controls.Add(this.Letter1);
            this.Controls.Add(this.Letter0);
            this.Name = "Form1";
            this.Text = "Form1";
            this.Load += new System.EventHandler(this.Form1_Load);
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Button Letter0;
        private System.Windows.Forms.Button Letter1;
        private System.Windows.Forms.Button Letter2;
        private System.Windows.Forms.Button Letter3;
        private System.Windows.Forms.Button Letter4;
        private System.Windows.Forms.Button GO_button;
        private System.Windows.Forms.Timer timer;
        private System.Windows.Forms.ListBox words_listBox;
    }
}

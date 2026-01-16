namespace ChessAI
{
    partial class ChatBox_Server_Form
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
            btnListen = new Button();
            richTextBox = new RichTextBox();
            SuspendLayout();
            // 
            // btnListen
            // 
            btnListen.BackColor = Color.PaleGreen;
            btnListen.Location = new Point(623, 25);
            btnListen.Name = "btnListen";
            btnListen.Size = new Size(124, 48);
            btnListen.TabIndex = 0;
            btnListen.Text = "Listen";
            btnListen.UseVisualStyleBackColor = false;
            btnListen.Click += btnListen_Click;
            // 
            // richTextBox
            // 
            richTextBox.Location = new Point(15, 96);
            richTextBox.Name = "richTextBox";
            richTextBox.ReadOnly = true;
            richTextBox.Size = new Size(773, 342);
            richTextBox.TabIndex = 1;
            richTextBox.Text = "";
            // 
            // ChatBox_Server_Form
            // 
            AutoScaleDimensions = new SizeF(8F, 20F);
            AutoScaleMode = AutoScaleMode.Font;
            BackColor = Color.Azure;
            ClientSize = new Size(800, 450);
            Controls.Add(richTextBox);
            Controls.Add(btnListen);
            Name = "ChatBox_Server_Form";
            Text = "ChatBox_Server_Form";
            Load += ServerForm_Load;
            ResumeLayout(false);
        }

        #endregion

        private Button btnListen;
        private RichTextBox richTextBox;
    }
}
namespace ConcussionTestingLab
{
    partial class FormMemoryTest
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
            this.pnlInstructions = new System.Windows.Forms.Panel();
            this.btnBegin = new System.Windows.Forms.Button();
            this.lblInstruction2 = new System.Windows.Forms.Label();
            this.lblInstructions = new System.Windows.Forms.Label();
            this.pnlWords = new System.Windows.Forms.Panel();
            this.lblwordList = new System.Windows.Forms.Label();
            this.pnlYesNo = new System.Windows.Forms.Panel();
            this.btnNo = new System.Windows.Forms.Button();
            this.btnYes = new System.Windows.Forms.Button();
            this.lblWord = new System.Windows.Forms.Label();
            this.lblQuestion = new System.Windows.Forms.Label();
            this.timer = new System.Windows.Forms.Timer(this.components);
            this.pnlInstructions2 = new System.Windows.Forms.Panel();
            this.btnNext = new System.Windows.Forms.Button();
            this.label1 = new System.Windows.Forms.Label();
            this.btnSubmit = new System.Windows.Forms.Button();
            this.pnlInstructions.SuspendLayout();
            this.pnlWords.SuspendLayout();
            this.pnlYesNo.SuspendLayout();
            this.pnlInstructions2.SuspendLayout();
            this.SuspendLayout();
            // 
            // pnlInstructions
            // 
            this.pnlInstructions.Controls.Add(this.btnBegin);
            this.pnlInstructions.Controls.Add(this.lblInstruction2);
            this.pnlInstructions.Controls.Add(this.lblInstructions);
            this.pnlInstructions.Font = new System.Drawing.Font("Microsoft Sans Serif", 21.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.pnlInstructions.Location = new System.Drawing.Point(39, 121);
            this.pnlInstructions.Margin = new System.Windows.Forms.Padding(4);
            this.pnlInstructions.Name = "pnlInstructions";
            this.pnlInstructions.Size = new System.Drawing.Size(1041, 186);
            this.pnlInstructions.TabIndex = 0;
            // 
            // btnBegin
            // 
            this.btnBegin.Location = new System.Drawing.Point(321, 111);
            this.btnBegin.Margin = new System.Windows.Forms.Padding(4);
            this.btnBegin.Name = "btnBegin";
            this.btnBegin.Size = new System.Drawing.Size(267, 70);
            this.btnBegin.TabIndex = 2;
            this.btnBegin.Text = "Begin Test";
            this.btnBegin.UseVisualStyleBackColor = true;
            this.btnBegin.Click += new System.EventHandler(this.btnBegin_Click);
            // 
            // lblInstruction2
            // 
            this.lblInstruction2.AutoSize = true;
            this.lblInstruction2.Location = new System.Drawing.Point(41, 58);
            this.lblInstruction2.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lblInstruction2.Name = "lblInstruction2";
            this.lblInstruction2.Size = new System.Drawing.Size(912, 42);
            this.lblInstruction2.TabIndex = 1;
            this.lblInstruction2.Text = "You will then be asked to remember the words shown.";
            // 
            // lblInstructions
            // 
            this.lblInstructions.AutoSize = true;
            this.lblInstructions.Location = new System.Drawing.Point(0, 0);
            this.lblInstructions.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lblInstructions.Name = "lblInstructions";
            this.lblInstructions.Size = new System.Drawing.Size(993, 42);
            this.lblInstructions.TabIndex = 0;
            this.lblInstructions.Text = "For this test a series of words will cycle through the screen.";
            // 
            // pnlWords
            // 
            this.pnlWords.Controls.Add(this.lblwordList);
            this.pnlWords.Location = new System.Drawing.Point(392, 15);
            this.pnlWords.Margin = new System.Windows.Forms.Padding(4);
            this.pnlWords.Name = "pnlWords";
            this.pnlWords.Size = new System.Drawing.Size(368, 298);
            this.pnlWords.TabIndex = 1;
            this.pnlWords.Visible = false;
            // 
            // lblwordList
            // 
            this.lblwordList.AutoSize = true;
            this.lblwordList.Font = new System.Drawing.Font("Microsoft Sans Serif", 24F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblwordList.Location = new System.Drawing.Point(123, 122);
            this.lblwordList.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lblwordList.Name = "lblwordList";
            this.lblwordList.Size = new System.Drawing.Size(120, 46);
            this.lblwordList.TabIndex = 8;
            this.lblwordList.Text = "Word";
            // 
            // pnlYesNo
            // 
            this.pnlYesNo.Controls.Add(this.btnNo);
            this.pnlYesNo.Controls.Add(this.btnYes);
            this.pnlYesNo.Controls.Add(this.lblWord);
            this.pnlYesNo.Controls.Add(this.lblQuestion);
            this.pnlYesNo.Location = new System.Drawing.Point(341, 21);
            this.pnlYesNo.Margin = new System.Windows.Forms.Padding(4);
            this.pnlYesNo.Name = "pnlYesNo";
            this.pnlYesNo.Size = new System.Drawing.Size(524, 286);
            this.pnlYesNo.TabIndex = 7;
            this.pnlYesNo.Visible = false;
            // 
            // btnNo
            // 
            this.btnNo.Font = new System.Drawing.Font("Microsoft Sans Serif", 15.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnNo.Location = new System.Drawing.Point(311, 180);
            this.btnNo.Margin = new System.Windows.Forms.Padding(4);
            this.btnNo.Name = "btnNo";
            this.btnNo.Size = new System.Drawing.Size(99, 55);
            this.btnNo.TabIndex = 3;
            this.btnNo.Text = "No";
            this.btnNo.UseVisualStyleBackColor = true;
            this.btnNo.Click += new System.EventHandler(this.btnNo_Click_1);
            // 
            // btnYes
            // 
            this.btnYes.Font = new System.Drawing.Font("Microsoft Sans Serif", 15.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnYes.Location = new System.Drawing.Point(125, 180);
            this.btnYes.Margin = new System.Windows.Forms.Padding(4);
            this.btnYes.Name = "btnYes";
            this.btnYes.Size = new System.Drawing.Size(99, 55);
            this.btnYes.TabIndex = 2;
            this.btnYes.Text = "Yes";
            this.btnYes.UseVisualStyleBackColor = true;
            this.btnYes.Click += new System.EventHandler(this.btnYes_Click);
            // 
            // lblWord
            // 
            this.lblWord.AutoSize = true;
            this.lblWord.Font = new System.Drawing.Font("Microsoft Sans Serif", 20.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblWord.Location = new System.Drawing.Point(219, 98);
            this.lblWord.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lblWord.Name = "lblWord";
            this.lblWord.Size = new System.Drawing.Size(98, 39);
            this.lblWord.TabIndex = 1;
            this.lblWord.Text = "Word";
            // 
            // lblQuestion
            // 
            this.lblQuestion.AutoSize = true;
            this.lblQuestion.Font = new System.Drawing.Font("Microsoft Sans Serif", 21.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblQuestion.Location = new System.Drawing.Point(33, 36);
            this.lblQuestion.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lblQuestion.Name = "lblQuestion";
            this.lblQuestion.Size = new System.Drawing.Size(468, 42);
            this.lblQuestion.TabIndex = 0;
            this.lblQuestion.Text = "Was this one of the words?";
            // 
            // timer
            // 
            this.timer.Interval = 2000;
            // 
            // pnlInstructions2
            // 
            this.pnlInstructions2.Controls.Add(this.btnNext);
            this.pnlInstructions2.Controls.Add(this.label1);
            this.pnlInstructions2.Location = new System.Drawing.Point(156, 33);
            this.pnlInstructions2.Margin = new System.Windows.Forms.Padding(4);
            this.pnlInstructions2.Name = "pnlInstructions2";
            this.pnlInstructions2.Size = new System.Drawing.Size(905, 277);
            this.pnlInstructions2.TabIndex = 9;
            this.pnlInstructions2.Visible = false;
            // 
            // btnNext
            // 
            this.btnNext.Font = new System.Drawing.Font("Microsoft Sans Serif", 15.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnNext.Location = new System.Drawing.Point(361, 165);
            this.btnNext.Margin = new System.Windows.Forms.Padding(4);
            this.btnNext.Name = "btnNext";
            this.btnNext.Size = new System.Drawing.Size(127, 53);
            this.btnNext.TabIndex = 1;
            this.btnNext.Text = "Next";
            this.btnNext.UseVisualStyleBackColor = true;
            this.btnNext.Click += new System.EventHandler(this.btnNext_Click);
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 15.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.Location = new System.Drawing.Point(57, 81);
            this.label1.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(747, 31);
            this.label1.TabIndex = 0;
            this.label1.Text = "Next you will be asked to remember what words were shown.";
            // 
            // btnSubmit
            // 
            this.btnSubmit.Location = new System.Drawing.Point(16, 368);
            this.btnSubmit.Margin = new System.Windows.Forms.Padding(4);
            this.btnSubmit.Name = "btnSubmit";
            this.btnSubmit.Size = new System.Drawing.Size(100, 28);
            this.btnSubmit.TabIndex = 10;
            this.btnSubmit.Text = "Submit";
            this.btnSubmit.UseVisualStyleBackColor = true;
            this.btnSubmit.Click += new System.EventHandler(this.btnSubmit_Click);
            // 
            // FormMemoryTest
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1109, 411);
            this.Controls.Add(this.btnSubmit);
            this.Controls.Add(this.pnlWords);
            this.Controls.Add(this.pnlInstructions2);
            this.Controls.Add(this.pnlYesNo);
            this.Controls.Add(this.pnlInstructions);
            this.Margin = new System.Windows.Forms.Padding(4);
            this.Name = "FormMemoryTest";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Form Memory Test";
            this.pnlInstructions.ResumeLayout(false);
            this.pnlInstructions.PerformLayout();
            this.pnlWords.ResumeLayout(false);
            this.pnlWords.PerformLayout();
            this.pnlYesNo.ResumeLayout(false);
            this.pnlYesNo.PerformLayout();
            this.pnlInstructions2.ResumeLayout(false);
            this.pnlInstructions2.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Panel pnlInstructions;
        private System.Windows.Forms.Label lblInstruction2;
        private System.Windows.Forms.Label lblInstructions;
        private System.Windows.Forms.Panel pnlWords;
        private System.Windows.Forms.Panel pnlYesNo;
        private System.Windows.Forms.Label lblWord;
        private System.Windows.Forms.Label lblQuestion;
        private System.Windows.Forms.Button btnBegin;
        private System.Windows.Forms.Timer timer;
        private System.Windows.Forms.Button btnNo;
        private System.Windows.Forms.Button btnYes;
        private System.Windows.Forms.Label lblwordList;
        private System.Windows.Forms.Panel pnlInstructions2;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Button btnNext;
        private System.Windows.Forms.Button btnSubmit;
    }
}
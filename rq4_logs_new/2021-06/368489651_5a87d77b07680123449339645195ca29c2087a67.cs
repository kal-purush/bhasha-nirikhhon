
namespace WindowsFormsApp2
{
    partial class EditDataFormForIncome
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
            this.closeEditIncomeButton = new System.Windows.Forms.Button();
            this.saveEditedIncomeButton = new System.Windows.Forms.Button();
            this.label3 = new System.Windows.Forms.Label();
            this.incomeDateInput = new System.Windows.Forms.DateTimePicker();
            this.incomeAmountInput = new System.Windows.Forms.NumericUpDown();
            this.label2 = new System.Windows.Forms.Label();
            this.editeIncomeCommentTextBox = new System.Windows.Forms.TextBox();
            this.label1 = new System.Windows.Forms.Label();
            this.deleteTargetIncomeBtn = new System.Windows.Forms.Button();
            ((System.ComponentModel.ISupportInitialize)(this.incomeAmountInput)).BeginInit();
            this.SuspendLayout();
            // 
            // closeEditIncomeButton
            // 
            this.closeEditIncomeButton.Location = new System.Drawing.Point(191, 210);
            this.closeEditIncomeButton.Name = "closeEditIncomeButton";
            this.closeEditIncomeButton.Size = new System.Drawing.Size(75, 23);
            this.closeEditIncomeButton.TabIndex = 19;
            this.closeEditIncomeButton.Text = "Cancel";
            this.closeEditIncomeButton.UseVisualStyleBackColor = true;
            // 
            // saveEditedIncomeButton
            // 
            this.saveEditedIncomeButton.Location = new System.Drawing.Point(191, 59);
            this.saveEditedIncomeButton.Name = "saveEditedIncomeButton";
            this.saveEditedIncomeButton.Size = new System.Drawing.Size(75, 23);
            this.saveEditedIncomeButton.TabIndex = 18;
            this.saveEditedIncomeButton.Text = "Save";
            this.saveEditedIncomeButton.UseVisualStyleBackColor = true;
            this.saveEditedIncomeButton.Click += new System.EventHandler(this.SaveEditedIncomeButton_Click);
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(42, 182);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(93, 13);
            this.label3.TabIndex = 17;
            this.label3.Text = "Enter income date";
            // 
            // incomeDateInput
            // 
            this.incomeDateInput.Location = new System.Drawing.Point(44, 213);
            this.incomeDateInput.Name = "incomeDateInput";
            this.incomeDateInput.Size = new System.Drawing.Size(121, 20);
            this.incomeDateInput.TabIndex = 16;
            // 
            // incomeAmountInput
            // 
            this.incomeAmountInput.Location = new System.Drawing.Point(45, 136);
            this.incomeAmountInput.Maximum = new decimal(new int[] {
            100000,
            0,
            0,
            0});
            this.incomeAmountInput.Name = "incomeAmountInput";
            this.incomeAmountInput.Size = new System.Drawing.Size(120, 20);
            this.incomeAmountInput.TabIndex = 15;
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(42, 101);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(119, 13);
            this.label2.TabIndex = 14;
            this.label2.Text = "Enter amount of income";
            // 
            // editeIncomeCommentTextBox
            // 
            this.editeIncomeCommentTextBox.Location = new System.Drawing.Point(45, 59);
            this.editeIncomeCommentTextBox.Name = "editeIncomeCommentTextBox";
            this.editeIncomeCommentTextBox.Size = new System.Drawing.Size(128, 20);
            this.editeIncomeCommentTextBox.TabIndex = 13;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(42, 30);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(78, 13);
            this.label1.TabIndex = 12;
            this.label1.Text = "Enter comment";
            // 
            // deleteTargetIncomeBtn
            // 
            this.deleteTargetIncomeBtn.Location = new System.Drawing.Point(191, 132);
            this.deleteTargetIncomeBtn.Name = "deleteTargetIncomeBtn";
            this.deleteTargetIncomeBtn.Size = new System.Drawing.Size(75, 23);
            this.deleteTargetIncomeBtn.TabIndex = 20;
            this.deleteTargetIncomeBtn.Text = "Delete";
            this.deleteTargetIncomeBtn.UseVisualStyleBackColor = true;
            this.deleteTargetIncomeBtn.Click += new System.EventHandler(this.DeleteTargetIncomeBtn_Click);
            // 
            // EditDataFormForIncome
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(278, 310);
            this.Controls.Add(this.deleteTargetIncomeBtn);
            this.Controls.Add(this.closeEditIncomeButton);
            this.Controls.Add(this.saveEditedIncomeButton);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.incomeDateInput);
            this.Controls.Add(this.incomeAmountInput);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.editeIncomeCommentTextBox);
            this.Controls.Add(this.label1);
            this.Name = "EditDataFormForIncome";
            this.Text = "EditDataFormForIncome";
            this.Load += new System.EventHandler(this.EditDataFormForIncome_Load);
            ((System.ComponentModel.ISupportInitialize)(this.incomeAmountInput)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button closeEditIncomeButton;
        private System.Windows.Forms.Button saveEditedIncomeButton;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.DateTimePicker incomeDateInput;
        private System.Windows.Forms.NumericUpDown incomeAmountInput;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.TextBox editeIncomeCommentTextBox;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Button deleteTargetIncomeBtn;
    }
}
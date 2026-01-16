namespace BMO.GameDevUnity.CSharp1.Pract8
{
    partial class FormMain
    {
        /// <summary>
        /// Обязательная переменная конструктора.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Освободить все используемые ресурсы.
        /// </summary>
        /// <param name="disposing">истинно, если управляемый ресурс должен быть удален; иначе ложно.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Код, автоматически созданный конструктором форм Windows

        /// <summary>
        /// Требуемый метод для поддержки конструктора — не изменяйте 
        /// содержимое этого метода с помощью редактора кода.
        /// </summary>
        private void InitializeComponent()
        {
            this.tBoxTask = new System.Windows.Forms.TextBox();
            this.numUpDownTask = new System.Windows.Forms.NumericUpDown();
            ((System.ComponentModel.ISupportInitialize)(this.numUpDownTask)).BeginInit();
            this.SuspendLayout();
            // 
            // tBoxTask
            // 
            this.tBoxTask.Location = new System.Drawing.Point(12, 82);
            this.tBoxTask.Name = "tBoxTask";
            this.tBoxTask.Size = new System.Drawing.Size(100, 22);
            this.tBoxTask.TabIndex = 0;
            this.tBoxTask.Text = "0";
            this.tBoxTask.TextChanged += new System.EventHandler(this.tBoxTask_TextChanged);
            // 
            // numUpDownTask
            // 
            this.numUpDownTask.Location = new System.Drawing.Point(170, 82);
            this.numUpDownTask.Name = "numUpDownTask";
            this.numUpDownTask.Size = new System.Drawing.Size(120, 22);
            this.numUpDownTask.TabIndex = 1;
            this.numUpDownTask.ValueChanged += new System.EventHandler(this.numUpDownTask_ValueChanged);
            // 
            // FormMain
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(302, 193);
            this.Controls.Add(this.numUpDownTask);
            this.Controls.Add(this.tBoxTask);
            this.Name = "FormMain";
            this.Text = "Второе задание";
            ((System.ComponentModel.ISupportInitialize)(this.numUpDownTask)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox tBoxTask;
        private System.Windows.Forms.NumericUpDown numUpDownTask;
    }
}

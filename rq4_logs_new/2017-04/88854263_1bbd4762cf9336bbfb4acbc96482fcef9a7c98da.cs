namespace MainForm
{
    partial class Form1
    {
        /// <summary>
        /// Требуется переменная конструктора.
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
        /// Обязательный метод для поддержки конструктора - не изменяйте
        /// содержимое данного метода при помощи редактора кода.
        /// </summary>
        private void InitializeComponent()
        {
            this.DGV = new System.Windows.Forms.DataGridView();
            this.outputControl1 = new MainForm.OutputControl();
            this.intialDateControl1 = new MainForm.IntialDateControl();
            this.CalculationButton = new System.Windows.Forms.Button();
            ((System.ComponentModel.ISupportInitialize)(this.DGV)).BeginInit();
            this.SuspendLayout();
            // 
            // DGV
            // 
            this.DGV.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.DGV.Location = new System.Drawing.Point(12, 159);
            this.DGV.Name = "DGV";
            this.DGV.Size = new System.Drawing.Size(457, 249);
            this.DGV.TabIndex = 2;
            // 
            // outputControl1
            // 
            this.outputControl1.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.outputControl1.Location = new System.Drawing.Point(1, 414);
            this.outputControl1.Name = "outputControl1";
            this.outputControl1.Size = new System.Drawing.Size(880, 132);
            this.outputControl1.TabIndex = 1;
            // 
            // intialDateControl1
            // 
            this.intialDateControl1.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.intialDateControl1.Location = new System.Drawing.Point(0, 0);
            this.intialDateControl1.Name = "intialDateControl1";
            this.intialDateControl1.Size = new System.Drawing.Size(881, 153);
            this.intialDateControl1.SomeValue = "";
            this.intialDateControl1.TabIndex = 0;
            // 
            // CalculationButton
            // 
            this.CalculationButton.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.CalculationButton.Location = new System.Drawing.Point(790, 25);
            this.CalculationButton.Name = "CalculationButton";
            this.CalculationButton.Size = new System.Drawing.Size(75, 23);
            this.CalculationButton.TabIndex = 16;
            this.CalculationButton.Text = "Расчёт";
            this.CalculationButton.UseVisualStyleBackColor = true;
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(877, 558);
            this.Controls.Add(this.CalculationButton);
            this.Controls.Add(this.DGV);
            this.Controls.Add(this.outputControl1);
            this.Controls.Add(this.intialDateControl1);
            this.Name = "Form1";
            this.Text = "Pisya";
            this.Load += new System.EventHandler(this.Form1_Load);
            ((System.ComponentModel.ISupportInitialize)(this.DGV)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion

        private IntialDateControl intialDateControl1;
        private OutputControl outputControl1;
        private System.Windows.Forms.DataGridView DGV;
        private System.Windows.Forms.Button CalculationButton;

    }
}

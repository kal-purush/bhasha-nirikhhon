namespace StudentsConvertCSVtoXML
{
    partial class FormConvert
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
            this.txtBoxPathCSV = new System.Windows.Forms.TextBox();
            this.btnChoosePathCSV = new System.Windows.Forms.Button();
            this.btnChoosePathXML = new System.Windows.Forms.Button();
            this.txtBoxPathXML = new System.Windows.Forms.TextBox();
            this.lblReady = new System.Windows.Forms.Label();
            this.btnConfirm = new System.Windows.Forms.Button();
            this.lblInfoCSV = new System.Windows.Forms.Label();
            this.lblInfoXML = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // txtBoxPathCSV
            // 
            this.txtBoxPathCSV.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.txtBoxPathCSV.Location = new System.Drawing.Point(13, 13);
            this.txtBoxPathCSV.Name = "txtBoxPathCSV";
            this.txtBoxPathCSV.Size = new System.Drawing.Size(597, 22);
            this.txtBoxPathCSV.TabIndex = 0;
            // 
            // btnChoosePathCSV
            // 
            this.btnChoosePathCSV.Location = new System.Drawing.Point(13, 42);
            this.btnChoosePathCSV.Name = "btnChoosePathCSV";
            this.btnChoosePathCSV.Size = new System.Drawing.Size(120, 30);
            this.btnChoosePathCSV.TabIndex = 1;
            this.btnChoosePathCSV.Text = "Выбор пути";
            this.btnChoosePathCSV.UseVisualStyleBackColor = true;
            this.btnChoosePathCSV.Click += new System.EventHandler(this.btnChoosePathCSV_Click);
            // 
            // btnChoosePathXML
            // 
            this.btnChoosePathXML.Location = new System.Drawing.Point(13, 136);
            this.btnChoosePathXML.Name = "btnChoosePathXML";
            this.btnChoosePathXML.Size = new System.Drawing.Size(120, 30);
            this.btnChoosePathXML.TabIndex = 3;
            this.btnChoosePathXML.Text = "Выбор пути";
            this.btnChoosePathXML.UseVisualStyleBackColor = true;
            this.btnChoosePathXML.Click += new System.EventHandler(this.btnChoosePathXML_Click);
            // 
            // txtBoxPathXML
            // 
            this.txtBoxPathXML.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.txtBoxPathXML.Location = new System.Drawing.Point(13, 107);
            this.txtBoxPathXML.Name = "txtBoxPathXML";
            this.txtBoxPathXML.Size = new System.Drawing.Size(597, 22);
            this.txtBoxPathXML.TabIndex = 2;
            // 
            // lblReady
            // 
            this.lblReady.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
            this.lblReady.AutoSize = true;
            this.lblReady.Font = new System.Drawing.Font("Microsoft Sans Serif", 19.8F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(204)));
            this.lblReady.Location = new System.Drawing.Point(12, 225);
            this.lblReady.Name = "lblReady";
            this.lblReady.Size = new System.Drawing.Size(190, 39);
            this.lblReady.TabIndex = 4;
            this.lblReady.Text = "Ожидание";
            // 
            // btnConfirm
            // 
            this.btnConfirm.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Right)));
            this.btnConfirm.Font = new System.Drawing.Font("Microsoft Sans Serif", 19.8F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(204)));
            this.btnConfirm.Location = new System.Drawing.Point(370, 211);
            this.btnConfirm.Name = "btnConfirm";
            this.btnConfirm.Size = new System.Drawing.Size(240, 50);
            this.btnConfirm.TabIndex = 5;
            this.btnConfirm.Text = "Выполнить";
            this.btnConfirm.UseVisualStyleBackColor = true;
            this.btnConfirm.Click += new System.EventHandler(this.btnConfirm_Click);
            // 
            // lblInfoCSV
            // 
            this.lblInfoCSV.AutoSize = true;
            this.lblInfoCSV.Location = new System.Drawing.Point(140, 42);
            this.lblInfoCSV.Name = "lblInfoCSV";
            this.lblInfoCSV.Size = new System.Drawing.Size(35, 17);
            this.lblInfoCSV.TabIndex = 6;
            this.lblInfoCSV.Text = "CSV";
            // 
            // lblInfoXML
            // 
            this.lblInfoXML.AutoSize = true;
            this.lblInfoXML.Location = new System.Drawing.Point(140, 136);
            this.lblInfoXML.Name = "lblInfoXML";
            this.lblInfoXML.Size = new System.Drawing.Size(36, 17);
            this.lblInfoXML.TabIndex = 7;
            this.lblInfoXML.Text = "XML";
            // 
            // FormConvert
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(622, 273);
            this.Controls.Add(this.lblInfoXML);
            this.Controls.Add(this.lblInfoCSV);
            this.Controls.Add(this.btnConfirm);
            this.Controls.Add(this.lblReady);
            this.Controls.Add(this.btnChoosePathXML);
            this.Controls.Add(this.txtBoxPathXML);
            this.Controls.Add(this.btnChoosePathCSV);
            this.Controls.Add(this.txtBoxPathCSV);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedDialog;
            this.Name = "FormConvert";
            this.Text = "Конвертирование CSV в XML";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox txtBoxPathCSV;
        private System.Windows.Forms.Button btnChoosePathCSV;
        private System.Windows.Forms.Button btnChoosePathXML;
        private System.Windows.Forms.TextBox txtBoxPathXML;
        private System.Windows.Forms.Label lblReady;
        private System.Windows.Forms.Button btnConfirm;
        private System.Windows.Forms.Label lblInfoCSV;
        private System.Windows.Forms.Label lblInfoXML;
    }
}

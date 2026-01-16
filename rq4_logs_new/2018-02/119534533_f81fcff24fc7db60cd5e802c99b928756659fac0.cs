namespace UrdsAppGestión.Presentacion.ComunidadesForms.ComunerosForms
{
    partial class FormImprimirMovimientos
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(FormImprimirMovimientos));
            this.maskedTextBoxini = new System.Windows.Forms.MaskedTextBox();
            this.maskedTextBoxfin = new System.Windows.Forms.MaskedTextBox();
            this.label1 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.ButtonImprimir = new System.Windows.Forms.Button();
            this.button1 = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // maskedTextBoxini
            // 
            this.maskedTextBoxini.Location = new System.Drawing.Point(98, 28);
            this.maskedTextBoxini.Mask = "00/00/0000";
            this.maskedTextBoxini.Name = "maskedTextBoxini";
            this.maskedTextBoxini.Size = new System.Drawing.Size(66, 20);
            this.maskedTextBoxini.TabIndex = 0;
            this.maskedTextBoxini.ValidatingType = typeof(System.DateTime);
            this.maskedTextBoxini.Leave += new System.EventHandler(this.maskedTextBoxini_Leave);
            // 
            // maskedTextBoxfin
            // 
            this.maskedTextBoxfin.Location = new System.Drawing.Point(98, 59);
            this.maskedTextBoxfin.Mask = "00/00/0000";
            this.maskedTextBoxfin.Name = "maskedTextBoxfin";
            this.maskedTextBoxfin.Size = new System.Drawing.Size(66, 20);
            this.maskedTextBoxfin.TabIndex = 1;
            this.maskedTextBoxfin.ValidatingType = typeof(System.DateTime);
            this.maskedTextBoxfin.Leave += new System.EventHandler(this.maskedTextBoxfin_Leave);
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.Location = new System.Drawing.Point(12, 31);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(67, 13);
            this.label1.TabIndex = 3;
            this.label1.Text = "Fecha inicio:";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label2.Location = new System.Drawing.Point(12, 59);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(54, 13);
            this.label2.TabIndex = 4;
            this.label2.Text = "Fecha fin:";
            // 
            // ButtonImprimir
            // 
            this.ButtonImprimir.Location = new System.Drawing.Point(71, 108);
            this.ButtonImprimir.Name = "ButtonImprimir";
            this.ButtonImprimir.Size = new System.Drawing.Size(75, 23);
            this.ButtonImprimir.TabIndex = 5;
            this.ButtonImprimir.Text = "Ver informe";
            this.ButtonImprimir.UseVisualStyleBackColor = true;
            this.ButtonImprimir.Click += new System.EventHandler(this.ButtonImprimir_Click);
            // 
            // button1
            // 
            this.button1.Location = new System.Drawing.Point(158, 108);
            this.button1.Name = "button1";
            this.button1.Size = new System.Drawing.Size(75, 23);
            this.button1.TabIndex = 6;
            this.button1.Text = "Cancelar";
            this.button1.UseVisualStyleBackColor = true;
            this.button1.Click += new System.EventHandler(this.button1_Click);
            // 
            // FormImprimirMovimientos
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(245, 158);
            this.Controls.Add(this.button1);
            this.Controls.Add(this.ButtonImprimir);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.maskedTextBoxfin);
            this.Controls.Add(this.maskedTextBoxini);
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.Name = "FormImprimirMovimientos";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Ver Informe";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.MaskedTextBox maskedTextBoxini;
        private System.Windows.Forms.MaskedTextBox maskedTextBoxfin;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Button ButtonImprimir;
        private System.Windows.Forms.Button button1;
    }
}
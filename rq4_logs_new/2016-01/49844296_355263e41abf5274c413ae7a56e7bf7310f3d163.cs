namespace heron_najam
{
    partial class Iznajmljivanje
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
            this.autoDataSet = new heron_najam.autoDataSet();
            this.Pregled = new System.Windows.Forms.Label();
            this.listaIznajmljivanja = new System.Windows.Forms.ListBox();
            ((System.ComponentModel.ISupportInitialize)(this.autoDataSet)).BeginInit();
            this.SuspendLayout();
            // 
            // autoDataSet
            // 
            this.autoDataSet.DataSetName = "autoDataSet";
            this.autoDataSet.SchemaSerializationMode = System.Data.SchemaSerializationMode.IncludeSchema;
            // 
            // Pregled
            // 
            this.Pregled.AutoSize = true;
            this.Pregled.Location = new System.Drawing.Point(25, 27);
            this.Pregled.Name = "Pregled";
            this.Pregled.Size = new System.Drawing.Size(75, 13);
            this.Pregled.TabIndex = 0;
            this.Pregled.Text = "Pregled osoba";
            // 
            // listaIznajmljivanja
            // 
            this.listaIznajmljivanja.FormattingEnabled = true;
            this.listaIznajmljivanja.Location = new System.Drawing.Point(28, 71);
            this.listaIznajmljivanja.Name = "listaIznajmljivanja";
            this.listaIznajmljivanja.Size = new System.Drawing.Size(521, 277);
            this.listaIznajmljivanja.TabIndex = 1;
            // 
            // Iznajmljivanje
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(585, 412);
            this.Controls.Add(this.listaIznajmljivanja);
            this.Controls.Add(this.Pregled);
            this.Name = "Iznajmljivanje";
            this.Text = "Iznajmljivanje";
            this.Load += new System.EventHandler(this.Iznajmljivanje_Load);
            ((System.ComponentModel.ISupportInitialize)(this.autoDataSet)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private autoDataSet autoDataSet;
        private System.Windows.Forms.Label Pregled;
        private System.Windows.Forms.ListBox listaIznajmljivanja;
    }
}

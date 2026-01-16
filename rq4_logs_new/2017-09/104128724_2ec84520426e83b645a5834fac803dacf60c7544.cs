namespace APD.Parallel
{
    partial class FrmPrincipalParallel
    {
        /// <summary>
        /// Variável de designer necessária.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Limpar os recursos que estão sendo usados.
        /// </summary>
        /// <param name="disposing">true se for necessário descartar os recursos gerenciados; caso contrário, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Código gerado pelo Windows Form Designer

        /// <summary>
        /// Método necessário para suporte ao Designer - não modifique 
        /// o conteúdo deste método com o editor de código.
        /// </summary>
        private void InitializeComponent()
        {
            this.pcbImg2 = new System.Windows.Forms.PictureBox();
            this.pcbImg3 = new System.Windows.Forms.PictureBox();
            this.gpbImagens = new System.Windows.Forms.GroupBox();
            this.BtnIniciar = new System.Windows.Forms.Button();
            this.pcbImg1 = new System.Windows.Forms.PictureBox();
            ((System.ComponentModel.ISupportInitialize)(this.pcbImg2)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.pcbImg3)).BeginInit();
            this.gpbImagens.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.pcbImg1)).BeginInit();
            this.SuspendLayout();
            // 
            // pcbImg2
            // 
            this.pcbImg2.BackColor = System.Drawing.Color.Transparent;
            this.pcbImg2.Location = new System.Drawing.Point(130, 31);
            this.pcbImg2.Name = "pcbImg2";
            this.pcbImg2.Size = new System.Drawing.Size(100, 100);
            this.pcbImg2.SizeMode = System.Windows.Forms.PictureBoxSizeMode.StretchImage;
            this.pcbImg2.TabIndex = 1;
            this.pcbImg2.TabStop = false;
            // 
            // pcbImg3
            // 
            this.pcbImg3.BackColor = System.Drawing.Color.Transparent;
            this.pcbImg3.Location = new System.Drawing.Point(236, 31);
            this.pcbImg3.Name = "pcbImg3";
            this.pcbImg3.Size = new System.Drawing.Size(100, 100);
            this.pcbImg3.SizeMode = System.Windows.Forms.PictureBoxSizeMode.StretchImage;
            this.pcbImg3.TabIndex = 2;
            this.pcbImg3.TabStop = false;
            // 
            // gpbImagens
            // 
            this.gpbImagens.Controls.Add(this.pcbImg1);
            this.gpbImagens.Controls.Add(this.pcbImg3);
            this.gpbImagens.Controls.Add(this.pcbImg2);
            this.gpbImagens.Location = new System.Drawing.Point(12, 12);
            this.gpbImagens.Name = "gpbImagens";
            this.gpbImagens.Size = new System.Drawing.Size(364, 163);
            this.gpbImagens.TabIndex = 3;
            this.gpbImagens.TabStop = false;
            this.gpbImagens.Text = "Imagens Utilizadas";
            // 
            // BtnIniciar
            // 
            this.BtnIniciar.Location = new System.Drawing.Point(248, 181);
            this.BtnIniciar.Name = "BtnIniciar";
            this.BtnIniciar.Size = new System.Drawing.Size(128, 38);
            this.BtnIniciar.TabIndex = 4;
            this.BtnIniciar.Text = "Iniciar Parallel";
            this.BtnIniciar.UseVisualStyleBackColor = true;
            this.BtnIniciar.Click += new System.EventHandler(this.BtnIniciar_Click);
            // 
            // pcbImg1
            // 
            this.pcbImg1.BackColor = System.Drawing.Color.Transparent;
            this.pcbImg1.Location = new System.Drawing.Point(24, 31);
            this.pcbImg1.Name = "pcbImg1";
            this.pcbImg1.Size = new System.Drawing.Size(100, 100);
            this.pcbImg1.SizeMode = System.Windows.Forms.PictureBoxSizeMode.StretchImage;
            this.pcbImg1.TabIndex = 3;
            this.pcbImg1.TabStop = false;
            // 
            // FrmPrincipal
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(399, 258);
            this.Controls.Add(this.BtnIniciar);
            this.Controls.Add(this.gpbImagens);
            this.Name = "FrmPrincipal";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Threads";
            this.Load += new System.EventHandler(this.FrmPrincipal_Load);
            ((System.ComponentModel.ISupportInitialize)(this.pcbImg2)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.pcbImg3)).EndInit();
            this.gpbImagens.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.pcbImg1)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion
        private System.Windows.Forms.PictureBox pcbImg2;
        private System.Windows.Forms.PictureBox pcbImg3;
        private System.Windows.Forms.GroupBox gpbImagens;
        private System.Windows.Forms.Button BtnIniciar;
        private System.Windows.Forms.PictureBox pcbImg1;
    }
}

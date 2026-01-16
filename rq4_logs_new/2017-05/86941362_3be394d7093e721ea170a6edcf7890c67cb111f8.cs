namespace Artigo
{
    partial class Artigo
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
            this.artigo_Titulo = new System.Windows.Forms.TextBox();
            this.artigo_Conteudo = new System.Windows.Forms.TextBox();
            this.label1 = new System.Windows.Forms.Label();
            this.btn_Submeter = new System.Windows.Forms.Button();
            this.btn_Cancelar = new System.Windows.Forms.Button();
            this.btn_Aprovar = new System.Windows.Forms.Button();
            this.btn_Reprovar = new System.Windows.Forms.Button();
            this.btn_Deletar = new System.Windows.Forms.Button();
            this.btn_Justificar = new System.Windows.Forms.Button();
            this.btn_listarArtigo = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // artigo_Titulo
            // 
            this.artigo_Titulo.Location = new System.Drawing.Point(23, 41);
            this.artigo_Titulo.Name = "artigo_Titulo";
            this.artigo_Titulo.Size = new System.Drawing.Size(322, 20);
            this.artigo_Titulo.TabIndex = 0;
            // 
            // artigo_Conteudo
            // 
            this.artigo_Conteudo.Location = new System.Drawing.Point(23, 93);
            this.artigo_Conteudo.Multiline = true;
            this.artigo_Conteudo.Name = "artigo_Conteudo";
            this.artigo_Conteudo.Size = new System.Drawing.Size(1152, 523);
            this.artigo_Conteudo.TabIndex = 1;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 14F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.Location = new System.Drawing.Point(19, 9);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(56, 24);
            this.label1.TabIndex = 2;
            this.label1.Text = "Titulo";
            // 
            // btn_Submeter
            // 
            this.btn_Submeter.Location = new System.Drawing.Point(1019, 633);
            this.btn_Submeter.Name = "btn_Submeter";
            this.btn_Submeter.Size = new System.Drawing.Size(158, 41);
            this.btn_Submeter.TabIndex = 4;
            this.btn_Submeter.Text = "Submeter";
            this.btn_Submeter.UseVisualStyleBackColor = true;
            this.btn_Submeter.Click += new System.EventHandler(this.btn_Submeter_Click);
            // 
            // btn_Cancelar
            // 
            this.btn_Cancelar.Location = new System.Drawing.Point(855, 633);
            this.btn_Cancelar.Name = "btn_Cancelar";
            this.btn_Cancelar.Size = new System.Drawing.Size(158, 41);
            this.btn_Cancelar.TabIndex = 5;
            this.btn_Cancelar.Text = "Cancelar";
            this.btn_Cancelar.UseVisualStyleBackColor = true;
            this.btn_Cancelar.Click += new System.EventHandler(this.btn_Cancelar_Click);
            // 
            // btn_Aprovar
            // 
            this.btn_Aprovar.Location = new System.Drawing.Point(1090, 12);
            this.btn_Aprovar.Name = "btn_Aprovar";
            this.btn_Aprovar.Size = new System.Drawing.Size(75, 23);
            this.btn_Aprovar.TabIndex = 6;
            this.btn_Aprovar.Text = "Aprovar";
            this.btn_Aprovar.UseVisualStyleBackColor = true;
            this.btn_Aprovar.Visible = false;
            this.btn_Aprovar.Click += new System.EventHandler(this.btn_Aprovar_Click);
            // 
            // btn_Reprovar
            // 
            this.btn_Reprovar.Location = new System.Drawing.Point(1009, 12);
            this.btn_Reprovar.Name = "btn_Reprovar";
            this.btn_Reprovar.Size = new System.Drawing.Size(75, 23);
            this.btn_Reprovar.TabIndex = 7;
            this.btn_Reprovar.Text = "Reprovar";
            this.btn_Reprovar.UseVisualStyleBackColor = true;
            this.btn_Reprovar.Visible = false;
            this.btn_Reprovar.Click += new System.EventHandler(this.btn_Reprovar_Click);
            // 
            // btn_Deletar
            // 
            this.btn_Deletar.Location = new System.Drawing.Point(928, 12);
            this.btn_Deletar.Name = "btn_Deletar";
            this.btn_Deletar.Size = new System.Drawing.Size(75, 23);
            this.btn_Deletar.TabIndex = 8;
            this.btn_Deletar.Text = "Deletar";
            this.btn_Deletar.UseVisualStyleBackColor = true;
            this.btn_Deletar.Visible = false;
            // 
            // btn_Justificar
            // 
            this.btn_Justificar.Location = new System.Drawing.Point(847, 12);
            this.btn_Justificar.Name = "btn_Justificar";
            this.btn_Justificar.Size = new System.Drawing.Size(75, 23);
            this.btn_Justificar.TabIndex = 9;
            this.btn_Justificar.Text = "Não rever";
            this.btn_Justificar.UseVisualStyleBackColor = true;
            this.btn_Justificar.Visible = false;
            this.btn_Justificar.Click += new System.EventHandler(this.btn_Justificar_Click);
            // 
            // btn_listarArtigo
            // 
            this.btn_listarArtigo.Location = new System.Drawing.Point(691, 633);
            this.btn_listarArtigo.Name = "btn_listarArtigo";
            this.btn_listarArtigo.Size = new System.Drawing.Size(158, 41);
            this.btn_listarArtigo.TabIndex = 10;
            this.btn_listarArtigo.Text = "Listar artigos";
            this.btn_listarArtigo.UseVisualStyleBackColor = true;
            this.btn_listarArtigo.Visible = false;
            this.btn_listarArtigo.Click += new System.EventHandler(this.btn_listarArtigo_Click);
            // 
            // Artigo
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1187, 686);
            this.Controls.Add(this.btn_listarArtigo);
            this.Controls.Add(this.btn_Justificar);
            this.Controls.Add(this.btn_Deletar);
            this.Controls.Add(this.btn_Reprovar);
            this.Controls.Add(this.btn_Aprovar);
            this.Controls.Add(this.btn_Cancelar);
            this.Controls.Add(this.btn_Submeter);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.artigo_Conteudo);
            this.Controls.Add(this.artigo_Titulo);
            this.Name = "Artigo";
            this.Text = "Artigo";
            this.WindowState = System.Windows.Forms.FormWindowState.Maximized;
            this.Load += new System.EventHandler(this.Artigo_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox artigo_Titulo;
        private System.Windows.Forms.TextBox artigo_Conteudo;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Button btn_Submeter;
        private System.Windows.Forms.Button btn_Cancelar;
        private System.Windows.Forms.Button btn_Aprovar;
        private System.Windows.Forms.Button btn_Reprovar;
        private System.Windows.Forms.Button btn_Deletar;
        private System.Windows.Forms.Button btn_Justificar;
        private System.Windows.Forms.Button btn_listarArtigo;
    }
}
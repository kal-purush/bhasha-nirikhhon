namespace Jardim_Zoológico
{
    partial class Menu_Principal
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(Menu_Principal));
            this.Painel_Cabeçalho = new System.Windows.Forms.Panel();
            this.btn_Titulo = new System.Windows.Forms.Label();
            this.btn_Fechar = new System.Windows.Forms.Button();
            this.btn_Clientes = new System.Windows.Forms.Button();
            this.painel_Menu = new System.Windows.Forms.Panel();
            this.btn_Informações = new System.Windows.Forms.Button();
            this.btn_Animais = new System.Windows.Forms.Button();
            this.btn_Funcionários = new System.Windows.Forms.Button();
            this.painel_Conteúdo = new System.Windows.Forms.Panel();
            this.Painel_Cabeçalho.SuspendLayout();
            this.painel_Menu.SuspendLayout();
            this.SuspendLayout();
            // 
            // Painel_Cabeçalho
            // 
            this.Painel_Cabeçalho.BackColor = System.Drawing.Color.Goldenrod;
            this.Painel_Cabeçalho.Controls.Add(this.btn_Titulo);
            this.Painel_Cabeçalho.Controls.Add(this.btn_Fechar);
            this.Painel_Cabeçalho.Dock = System.Windows.Forms.DockStyle.Top;
            this.Painel_Cabeçalho.Location = new System.Drawing.Point(0, 0);
            this.Painel_Cabeçalho.Name = "Painel_Cabeçalho";
            this.Painel_Cabeçalho.Size = new System.Drawing.Size(800, 100);
            this.Painel_Cabeçalho.TabIndex = 0;
            // 
            // btn_Titulo
            // 
            this.btn_Titulo.AutoSize = true;
            this.btn_Titulo.ForeColor = System.Drawing.Color.Black;
            this.btn_Titulo.Location = new System.Drawing.Point(200, 41);
            this.btn_Titulo.Name = "btn_Titulo";
            this.btn_Titulo.Size = new System.Drawing.Size(90, 13);
            this.btn_Titulo.TabIndex = 4;
            this.btn_Titulo.Text = "Jardim_Zoológico";
            // 
            // btn_Fechar
            // 
            this.btn_Fechar.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("btn_Fechar.BackgroundImage")));
            this.btn_Fechar.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Zoom;
            this.btn_Fechar.FlatAppearance.BorderColor = System.Drawing.Color.White;
            this.btn_Fechar.FlatAppearance.BorderSize = 0;
            this.btn_Fechar.FlatAppearance.MouseDownBackColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.btn_Fechar.FlatAppearance.MouseOverBackColor = System.Drawing.Color.Red;
            this.btn_Fechar.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btn_Fechar.Location = new System.Drawing.Point(737, 27);
            this.btn_Fechar.Name = "btn_Fechar";
            this.btn_Fechar.Size = new System.Drawing.Size(51, 40);
            this.btn_Fechar.TabIndex = 1;
            this.btn_Fechar.UseVisualStyleBackColor = true;
            this.btn_Fechar.Click += new System.EventHandler(this.btn_Fechar_Click);
            // 
            // btn_Clientes
            // 
            this.btn_Clientes.BackColor = System.Drawing.Color.White;
            this.btn_Clientes.FlatAppearance.BorderColor = System.Drawing.Color.White;
            this.btn_Clientes.FlatAppearance.BorderSize = 0;
            this.btn_Clientes.FlatAppearance.MouseDownBackColor = System.Drawing.Color.White;
            this.btn_Clientes.FlatAppearance.MouseOverBackColor = System.Drawing.Color.Red;
            this.btn_Clientes.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btn_Clientes.Font = new System.Drawing.Font("Microsoft Sans Serif", 10F);
            this.btn_Clientes.ForeColor = System.Drawing.Color.Black;
            this.btn_Clientes.Location = new System.Drawing.Point(9, 27);
            this.btn_Clientes.Name = "btn_Clientes";
            this.btn_Clientes.Size = new System.Drawing.Size(170, 50);
            this.btn_Clientes.TabIndex = 1;
            this.btn_Clientes.Text = "Clientes";
            this.btn_Clientes.UseVisualStyleBackColor = false;
            // 
            // painel_Menu
            // 
            this.painel_Menu.BackColor = System.Drawing.SystemColors.AppWorkspace;
            this.painel_Menu.Controls.Add(this.btn_Informações);
            this.painel_Menu.Controls.Add(this.btn_Animais);
            this.painel_Menu.Controls.Add(this.btn_Funcionários);
            this.painel_Menu.Controls.Add(this.btn_Clientes);
            this.painel_Menu.Location = new System.Drawing.Point(0, 100);
            this.painel_Menu.Name = "painel_Menu";
            this.painel_Menu.Size = new System.Drawing.Size(200, 350);
            this.painel_Menu.TabIndex = 5;
            // 
            // btn_Informações
            // 
            this.btn_Informações.BackColor = System.Drawing.Color.White;
            this.btn_Informações.FlatAppearance.BorderColor = System.Drawing.Color.White;
            this.btn_Informações.FlatAppearance.BorderSize = 0;
            this.btn_Informações.FlatAppearance.MouseDownBackColor = System.Drawing.Color.White;
            this.btn_Informações.FlatAppearance.MouseOverBackColor = System.Drawing.Color.Red;
            this.btn_Informações.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btn_Informações.Font = new System.Drawing.Font("Microsoft Sans Serif", 10F);
            this.btn_Informações.ForeColor = System.Drawing.Color.Black;
            this.btn_Informações.Location = new System.Drawing.Point(10, 265);
            this.btn_Informações.Name = "btn_Informações";
            this.btn_Informações.Size = new System.Drawing.Size(170, 50);
            this.btn_Informações.TabIndex = 6;
            this.btn_Informações.Text = "Informações";
            this.btn_Informações.UseVisualStyleBackColor = false;
            // 
            // btn_Animais
            // 
            this.btn_Animais.BackColor = System.Drawing.Color.White;
            this.btn_Animais.FlatAppearance.BorderColor = System.Drawing.Color.White;
            this.btn_Animais.FlatAppearance.BorderSize = 0;
            this.btn_Animais.FlatAppearance.MouseDownBackColor = System.Drawing.Color.White;
            this.btn_Animais.FlatAppearance.MouseOverBackColor = System.Drawing.Color.Red;
            this.btn_Animais.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btn_Animais.Font = new System.Drawing.Font("Microsoft Sans Serif", 10F);
            this.btn_Animais.ForeColor = System.Drawing.Color.Black;
            this.btn_Animais.Location = new System.Drawing.Point(10, 190);
            this.btn_Animais.Name = "btn_Animais";
            this.btn_Animais.Size = new System.Drawing.Size(170, 50);
            this.btn_Animais.TabIndex = 6;
            this.btn_Animais.Text = "Animais";
            this.btn_Animais.UseVisualStyleBackColor = false;
            // 
            // btn_Funcionários
            // 
            this.btn_Funcionários.BackColor = System.Drawing.Color.White;
            this.btn_Funcionários.FlatAppearance.BorderColor = System.Drawing.Color.White;
            this.btn_Funcionários.FlatAppearance.BorderSize = 0;
            this.btn_Funcionários.FlatAppearance.MouseDownBackColor = System.Drawing.Color.White;
            this.btn_Funcionários.FlatAppearance.MouseOverBackColor = System.Drawing.Color.Red;
            this.btn_Funcionários.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btn_Funcionários.Font = new System.Drawing.Font("Microsoft Sans Serif", 10F);
            this.btn_Funcionários.ForeColor = System.Drawing.Color.Black;
            this.btn_Funcionários.Location = new System.Drawing.Point(9, 109);
            this.btn_Funcionários.Name = "btn_Funcionários";
            this.btn_Funcionários.Size = new System.Drawing.Size(170, 50);
            this.btn_Funcionários.TabIndex = 6;
            this.btn_Funcionários.Text = "Funcionários";
            this.btn_Funcionários.UseVisualStyleBackColor = false;
            // 
            // painel_Conteúdo
            // 
            this.painel_Conteúdo.Dock = System.Windows.Forms.DockStyle.Right;
            this.painel_Conteúdo.Location = new System.Drawing.Point(188, 100);
            this.painel_Conteúdo.Name = "painel_Conteúdo";
            this.painel_Conteúdo.Size = new System.Drawing.Size(612, 350);
            this.painel_Conteúdo.TabIndex = 6;
            // 
            // Menu_Principal
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 450);
            this.Controls.Add(this.painel_Conteúdo);
            this.Controls.Add(this.painel_Menu);
            this.Controls.Add(this.Painel_Cabeçalho);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.None;
            this.Name = "Menu_Principal";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Form1";
            this.Painel_Cabeçalho.ResumeLayout(false);
            this.Painel_Cabeçalho.PerformLayout();
            this.painel_Menu.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Panel Painel_Cabeçalho;
        private System.Windows.Forms.Button btn_Fechar;
        private System.Windows.Forms.Label btn_Titulo;
        private System.Windows.Forms.Button btn_Clientes;
        private System.Windows.Forms.Panel painel_Menu;
        private System.Windows.Forms.Button btn_Informações;
        private System.Windows.Forms.Button btn_Animais;
        private System.Windows.Forms.Button btn_Funcionários;
        private System.Windows.Forms.Panel painel_Conteúdo;
    }
}

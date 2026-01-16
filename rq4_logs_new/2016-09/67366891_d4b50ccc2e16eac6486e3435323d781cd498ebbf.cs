namespace BuscaBO
{
    partial class AdicionarPesquisaForm
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
            this.label1 = new System.Windows.Forms.Label();
            this.NomePesquisaTextBox = new System.Windows.Forms.TextBox();
            this.label2 = new System.Windows.Forms.Label();
            this.ArquivoEscolhidoLabel = new System.Windows.Forms.Label();
            this.EscolherArquivoButton = new System.Windows.Forms.Button();
            this.ArquivoPanel = new System.Windows.Forms.Panel();
            this.CamposPanel = new System.Windows.Forms.Panel();
            this.TituloExibicao2TextBox = new System.Windows.Forms.TextBox();
            this.TituloExibicao1TextBox = new System.Windows.Forms.TextBox();
            this.ConfirmarButton = new System.Windows.Forms.Button();
            this.CampoExibicao2ComboBox = new System.Windows.Forms.ComboBox();
            this.CampoExibicao1ComboBox = new System.Windows.Forms.ComboBox();
            this.label6 = new System.Windows.Forms.Label();
            this.label5 = new System.Windows.Forms.Label();
            this.CampoBuscaComboBox = new System.Windows.Forms.ComboBox();
            this.label4 = new System.Windows.Forms.Label();
            this.NomeTabelaComboBox = new System.Windows.Forms.ComboBox();
            this.label3 = new System.Windows.Forms.Label();
            this.VoltarButton = new System.Windows.Forms.Button();
            this.ArquivoPanel.SuspendLayout();
            this.CamposPanel.SuspendLayout();
            this.SuspendLayout();
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(13, 16);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(98, 13);
            this.label1.TabIndex = 0;
            this.label1.Text = "Nome da pesquisa:";
            // 
            // NomePesquisaTextBox
            // 
            this.NomePesquisaTextBox.Location = new System.Drawing.Point(115, 13);
            this.NomePesquisaTextBox.Name = "NomePesquisaTextBox";
            this.NomePesquisaTextBox.Size = new System.Drawing.Size(390, 20);
            this.NomePesquisaTextBox.TabIndex = 1;
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(164, 51);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(46, 13);
            this.label2.TabIndex = 2;
            this.label2.Text = "Arquivo:";
            // 
            // ArquivoEscolhidoLabel
            // 
            this.ArquivoEscolhidoLabel.AutoEllipsis = true;
            this.ArquivoEscolhidoLabel.AutoSize = true;
            this.ArquivoEscolhidoLabel.Location = new System.Drawing.Point(216, 51);
            this.ArquivoEscolhidoLabel.Name = "ArquivoEscolhidoLabel";
            this.ArquivoEscolhidoLabel.Size = new System.Drawing.Size(0, 13);
            this.ArquivoEscolhidoLabel.TabIndex = 3;
            // 
            // EscolherArquivoButton
            // 
            this.EscolherArquivoButton.Location = new System.Drawing.Point(16, 46);
            this.EscolherArquivoButton.Name = "EscolherArquivoButton";
            this.EscolherArquivoButton.Size = new System.Drawing.Size(142, 23);
            this.EscolherArquivoButton.TabIndex = 4;
            this.EscolherArquivoButton.Text = "Escolher &arquivo...";
            this.EscolherArquivoButton.UseVisualStyleBackColor = true;
            this.EscolherArquivoButton.Click += new System.EventHandler(this.EscolherArquivoButton_Click);
            // 
            // ArquivoPanel
            // 
            this.ArquivoPanel.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.ArquivoPanel.Controls.Add(this.CamposPanel);
            this.ArquivoPanel.Controls.Add(this.NomeTabelaComboBox);
            this.ArquivoPanel.Controls.Add(this.label3);
            this.ArquivoPanel.Enabled = false;
            this.ArquivoPanel.Location = new System.Drawing.Point(16, 75);
            this.ArquivoPanel.Name = "ArquivoPanel";
            this.ArquivoPanel.Size = new System.Drawing.Size(489, 167);
            this.ArquivoPanel.TabIndex = 5;
            // 
            // CamposPanel
            // 
            this.CamposPanel.Controls.Add(this.TituloExibicao2TextBox);
            this.CamposPanel.Controls.Add(this.TituloExibicao1TextBox);
            this.CamposPanel.Controls.Add(this.ConfirmarButton);
            this.CamposPanel.Controls.Add(this.CampoExibicao2ComboBox);
            this.CamposPanel.Controls.Add(this.CampoExibicao1ComboBox);
            this.CamposPanel.Controls.Add(this.label6);
            this.CamposPanel.Controls.Add(this.label5);
            this.CamposPanel.Controls.Add(this.CampoBuscaComboBox);
            this.CamposPanel.Controls.Add(this.label4);
            this.CamposPanel.Enabled = false;
            this.CamposPanel.Location = new System.Drawing.Point(4, 44);
            this.CamposPanel.Name = "CamposPanel";
            this.CamposPanel.Size = new System.Drawing.Size(479, 119);
            this.CamposPanel.TabIndex = 2;
            // 
            // TituloExibicao2TextBox
            // 
            this.TituloExibicao2TextBox.Location = new System.Drawing.Point(352, 65);
            this.TituloExibicao2TextBox.Name = "TituloExibicao2TextBox";
            this.TituloExibicao2TextBox.Size = new System.Drawing.Size(108, 20);
            this.TituloExibicao2TextBox.TabIndex = 8;
            // 
            // TituloExibicao1TextBox
            // 
            this.TituloExibicao1TextBox.Location = new System.Drawing.Point(351, 39);
            this.TituloExibicao1TextBox.Name = "TituloExibicao1TextBox";
            this.TituloExibicao1TextBox.Size = new System.Drawing.Size(109, 20);
            this.TituloExibicao1TextBox.TabIndex = 7;
            // 
            // ConfirmarButton
            // 
            this.ConfirmarButton.Location = new System.Drawing.Point(353, 92);
            this.ConfirmarButton.Name = "ConfirmarButton";
            this.ConfirmarButton.Size = new System.Drawing.Size(120, 23);
            this.ConfirmarButton.TabIndex = 6;
            this.ConfirmarButton.Text = "&Confirmar";
            this.ConfirmarButton.UseVisualStyleBackColor = true;
            this.ConfirmarButton.Click += new System.EventHandler(this.ConfirmarButton_Click);
            // 
            // CampoExibicao2ComboBox
            // 
            this.CampoExibicao2ComboBox.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.CampoExibicao2ComboBox.FormattingEnabled = true;
            this.CampoExibicao2ComboBox.Location = new System.Drawing.Point(113, 65);
            this.CampoExibicao2ComboBox.Name = "CampoExibicao2ComboBox";
            this.CampoExibicao2ComboBox.Size = new System.Drawing.Size(232, 21);
            this.CampoExibicao2ComboBox.TabIndex = 5;
            this.CampoExibicao2ComboBox.SelectedIndexChanged += new System.EventHandler(this.CampoExibicao2ComboBox_SelectedIndexChanged);
            // 
            // CampoExibicao1ComboBox
            // 
            this.CampoExibicao1ComboBox.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.CampoExibicao1ComboBox.FormattingEnabled = true;
            this.CampoExibicao1ComboBox.Location = new System.Drawing.Point(113, 39);
            this.CampoExibicao1ComboBox.Name = "CampoExibicao1ComboBox";
            this.CampoExibicao1ComboBox.Size = new System.Drawing.Size(232, 21);
            this.CampoExibicao1ComboBox.TabIndex = 4;
            this.CampoExibicao1ComboBox.SelectedIndexChanged += new System.EventHandler(this.CampoExibicao1ComboBox_SelectedIndexChanged);
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Location = new System.Drawing.Point(7, 68);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(100, 13);
            this.label6.TabIndex = 3;
            this.label6.Text = "Coluna exibição (2):";
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Location = new System.Drawing.Point(7, 42);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(100, 13);
            this.label5.TabIndex = 2;
            this.label5.Text = "Coluna exibição (1):";
            // 
            // CampoBuscaComboBox
            // 
            this.CampoBuscaComboBox.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.CampoBuscaComboBox.FormattingEnabled = true;
            this.CampoBuscaComboBox.Location = new System.Drawing.Point(100, 9);
            this.CampoBuscaComboBox.Name = "CampoBuscaComboBox";
            this.CampoBuscaComboBox.Size = new System.Drawing.Size(360, 21);
            this.CampoBuscaComboBox.TabIndex = 1;
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(3, 12);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(90, 13);
            this.label4.TabIndex = 0;
            this.label4.Text = "Campo de busca:";
            // 
            // NomeTabelaComboBox
            // 
            this.NomeTabelaComboBox.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.NomeTabelaComboBox.FormattingEnabled = true;
            this.NomeTabelaComboBox.Location = new System.Drawing.Point(52, 10);
            this.NomeTabelaComboBox.Name = "NomeTabelaComboBox";
            this.NomeTabelaComboBox.Size = new System.Drawing.Size(297, 21);
            this.NomeTabelaComboBox.TabIndex = 1;
            this.NomeTabelaComboBox.SelectedIndexChanged += new System.EventHandler(this.NomeTabelaComboBox_SelectedIndexChanged);
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(3, 13);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(43, 13);
            this.label3.TabIndex = 0;
            this.label3.Text = "Tabela:";
            // 
            // VoltarButton
            // 
            this.VoltarButton.Location = new System.Drawing.Point(419, 252);
            this.VoltarButton.Name = "VoltarButton";
            this.VoltarButton.Size = new System.Drawing.Size(75, 23);
            this.VoltarButton.TabIndex = 6;
            this.VoltarButton.Text = "&Voltar";
            this.VoltarButton.UseVisualStyleBackColor = true;
            this.VoltarButton.Click += new System.EventHandler(this.VoltarButton_Click);
            // 
            // AdicionarPesquisaForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(515, 282);
            this.Controls.Add(this.VoltarButton);
            this.Controls.Add(this.ArquivoPanel);
            this.Controls.Add(this.EscolherArquivoButton);
            this.Controls.Add(this.ArquivoEscolhidoLabel);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.NomePesquisaTextBox);
            this.Controls.Add(this.label1);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedSingle;
            this.Name = "AdicionarPesquisaForm";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterParent;
            this.Text = "Adicionar pesquisa";
            this.ArquivoPanel.ResumeLayout(false);
            this.ArquivoPanel.PerformLayout();
            this.CamposPanel.ResumeLayout(false);
            this.CamposPanel.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.TextBox NomePesquisaTextBox;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label ArquivoEscolhidoLabel;
        private System.Windows.Forms.Button EscolherArquivoButton;
        private System.Windows.Forms.Panel ArquivoPanel;
        private System.Windows.Forms.Panel CamposPanel;
        private System.Windows.Forms.Button ConfirmarButton;
        private System.Windows.Forms.ComboBox CampoExibicao2ComboBox;
        private System.Windows.Forms.ComboBox CampoExibicao1ComboBox;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.ComboBox CampoBuscaComboBox;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.ComboBox NomeTabelaComboBox;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.Button VoltarButton;
        private System.Windows.Forms.TextBox TituloExibicao2TextBox;
        private System.Windows.Forms.TextBox TituloExibicao1TextBox;
    }
}
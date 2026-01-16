namespace ProtoMine.View
{
    partial class LojaPedidos
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
            this.tabela = new System.Windows.Forms.DataGridView();
            this.cbTIpo = new System.Windows.Forms.ComboBox();
            ((System.ComponentModel.ISupportInitialize)(this.tabela)).BeginInit();
            this.SuspendLayout();
            // 
            // tabela
            // 
            this.tabela.AllowUserToAddRows = false;
            this.tabela.AutoSizeColumnsMode = System.Windows.Forms.DataGridViewAutoSizeColumnsMode.Fill;
            this.tabela.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.tabela.Location = new System.Drawing.Point(349, 102);
            this.tabela.Name = "tabela";
            this.tabela.Size = new System.Drawing.Size(450, 267);
            this.tabela.TabIndex = 0;
            // 
            // cbTIpo
            // 
            this.cbTIpo.BackColor = System.Drawing.SystemColors.ButtonHighlight;
            this.cbTIpo.Font = new System.Drawing.Font("Roboto", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.cbTIpo.FormattingEnabled = true;
            this.cbTIpo.Location = new System.Drawing.Point(541, 67);
            this.cbTIpo.Name = "cbTIpo";
            this.cbTIpo.Size = new System.Drawing.Size(258, 27);
            this.cbTIpo.TabIndex = 2;
            this.cbTIpo.SelectedIndexChanged += new System.EventHandler(this.AlterarBusca);
            // 
            // LojaPedidos
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackgroundImage = global::ProtoMine.Properties.Resources.Loja_SemItens;
            this.ClientSize = new System.Drawing.Size(880, 620);
            this.Controls.Add(this.cbTIpo);
            this.Controls.Add(this.tabela);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.None;
            this.Name = "LojaPedidos";
            this.Text = "LojaPedidos";
            ((System.ComponentModel.ISupportInitialize)(this.tabela)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.DataGridView tabela;
        private System.Windows.Forms.ComboBox cbTIpo;
    }
}
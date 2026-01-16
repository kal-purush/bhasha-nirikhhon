namespace TCM_Pharmex
{
    partial class FrmConfig
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(FrmConfig));
            this.btnAjuda = new MetroFramework.Controls.MetroButton();
            this.btnVersao = new MetroFramework.Controls.MetroButton();
            this.btnCred = new MetroFramework.Controls.MetroButton();
            this.btnSenha = new MetroFramework.Controls.MetroButton();
            this.btnTela = new MetroFramework.Controls.MetroButton();
            this.btnAplicar = new MetroFramework.Controls.MetroButton();
            this.btnVoltar = new MetroFramework.Controls.MetroButton();
            this.cbxTema = new MetroFramework.Controls.MetroComboBox();
            this.cbxIdiom = new MetroFramework.Controls.MetroComboBox();
            this.cbxRes = new MetroFramework.Controls.MetroComboBox();
            this.SuspendLayout();
            // 
            // btnAjuda
            // 
            resources.ApplyResources(this.btnAjuda, "btnAjuda");
            this.btnAjuda.Name = "btnAjuda";
            this.btnAjuda.UseSelectable = true;
            // 
            // btnVersao
            // 
            resources.ApplyResources(this.btnVersao, "btnVersao");
            this.btnVersao.Name = "btnVersao";
            this.btnVersao.UseSelectable = true;
            // 
            // btnCred
            // 
            resources.ApplyResources(this.btnCred, "btnCred");
            this.btnCred.Name = "btnCred";
            this.btnCred.UseSelectable = true;
            // 
            // btnSenha
            // 
            resources.ApplyResources(this.btnSenha, "btnSenha");
            this.btnSenha.Name = "btnSenha";
            this.btnSenha.UseSelectable = true;
            // 
            // btnTela
            // 
            resources.ApplyResources(this.btnTela, "btnTela");
            this.btnTela.Name = "btnTela";
            this.btnTela.UseSelectable = true;
            // 
            // btnAplicar
            // 
            resources.ApplyResources(this.btnAplicar, "btnAplicar");
            this.btnAplicar.Name = "btnAplicar";
            this.btnAplicar.UseSelectable = true;
            this.btnAplicar.Click += new System.EventHandler(this.btnAplicar_Click);
            // 
            // btnVoltar
            // 
            resources.ApplyResources(this.btnVoltar, "btnVoltar");
            this.btnVoltar.Name = "btnVoltar";
            this.btnVoltar.UseSelectable = true;
            this.btnVoltar.Click += new System.EventHandler(this.btnVoltar_Click);
            // 
            // cbxTema
            // 
            this.cbxTema.FormattingEnabled = true;
            resources.ApplyResources(this.cbxTema, "cbxTema");
            this.cbxTema.Name = "cbxTema";
            this.cbxTema.PromptText = "Tema";
            this.cbxTema.UseSelectable = true;
            // 
            // cbxIdiom
            // 
            this.cbxIdiom.FormattingEnabled = true;
            resources.ApplyResources(this.cbxIdiom, "cbxIdiom");
            this.cbxIdiom.Name = "cbxIdiom";
            this.cbxIdiom.PromptText = "Idioma";
            this.cbxIdiom.UseSelectable = true;
            this.cbxIdiom.SelectedIndexChanged += new System.EventHandler(this.cbxIdiom_SelectedIndexChanged);
            // 
            // cbxRes
            // 
            this.cbxRes.FormattingEnabled = true;
            resources.ApplyResources(this.cbxRes, "cbxRes");
            this.cbxRes.Name = "cbxRes";
            this.cbxRes.PromptText = "Resolução";
            this.cbxRes.UseSelectable = true;
            // 
            // FrmConfig
            // 
            resources.ApplyResources(this, "$this");
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.cbxRes);
            this.Controls.Add(this.cbxIdiom);
            this.Controls.Add(this.cbxTema);
            this.Controls.Add(this.btnVoltar);
            this.Controls.Add(this.btnAplicar);
            this.Controls.Add(this.btnTela);
            this.Controls.Add(this.btnSenha);
            this.Controls.Add(this.btnCred);
            this.Controls.Add(this.btnVersao);
            this.Controls.Add(this.btnAjuda);
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "FrmConfig";
            this.Resizable = false;
            this.TextAlign = MetroFramework.Forms.MetroFormTextAlign.Center;
            this.Load += new System.EventHandler(this.FrmConfig_Load);
            this.ResumeLayout(false);

        }

        #endregion

        private MetroFramework.Controls.MetroButton btnAjuda;
        private MetroFramework.Controls.MetroButton btnVersao;
        private MetroFramework.Controls.MetroButton btnCred;
        private MetroFramework.Controls.MetroButton btnSenha;
        private MetroFramework.Controls.MetroButton btnTela;
        private MetroFramework.Controls.MetroButton btnAplicar;
        private MetroFramework.Controls.MetroButton btnVoltar;
        private MetroFramework.Controls.MetroComboBox cbxTema;
        private MetroFramework.Controls.MetroComboBox cbxIdiom;
        private MetroFramework.Controls.MetroComboBox cbxRes;
    }
}
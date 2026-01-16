namespace TCC
{
    partial class frmEditPessoa
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
            this.btnCanc = new MetroFramework.Controls.MetroButton();
            this.btnGravar = new MetroFramework.Controls.MetroButton();
            this.gbxPaciente = new System.Windows.Forms.GroupBox();
            this.btnAssociar = new MetroFramework.Controls.MetroButton();
            this.txtNomePaci = new MetroFramework.Controls.MetroTextBox();
            this.labelid = new MetroFramework.Controls.MetroLabel();
            this.txtId = new MetroFramework.Controls.MetroTextBox();
            this.metroLabel6 = new MetroFramework.Controls.MetroLabel();
            this.cbxTipo = new MetroFramework.Controls.MetroComboBox();
            this.metroLabel5 = new MetroFramework.Controls.MetroLabel();
            this.txtTelefone = new MetroFramework.Controls.MetroTextBox();
            this.metroLabel4 = new MetroFramework.Controls.MetroLabel();
            this.txtEmail = new MetroFramework.Controls.MetroTextBox();
            this.metroLabel3 = new MetroFramework.Controls.MetroLabel();
            this.txtCpf = new MetroFramework.Controls.MetroTextBox();
            this.metroLabel2 = new MetroFramework.Controls.MetroLabel();
            this.txtNome = new MetroFramework.Controls.MetroTextBox();
            this.metroLabel1 = new MetroFramework.Controls.MetroLabel();
            this.gbxPaciente.SuspendLayout();
            this.SuspendLayout();
            // 
            // btnCanc
            // 
            this.btnCanc.Location = new System.Drawing.Point(290, 388);
            this.btnCanc.Name = "btnCanc";
            this.btnCanc.Size = new System.Drawing.Size(88, 45);
            this.btnCanc.TabIndex = 62;
            this.btnCanc.Text = "Cancelar";
            this.btnCanc.UseSelectable = true;
            // 
            // btnGravar
            // 
            this.btnGravar.Location = new System.Drawing.Point(117, 388);
            this.btnGravar.Name = "btnGravar";
            this.btnGravar.Size = new System.Drawing.Size(88, 45);
            this.btnGravar.TabIndex = 61;
            this.btnGravar.Text = "Confirmar";
            this.btnGravar.UseSelectable = true;
            // 
            // gbxPaciente
            // 
            this.gbxPaciente.Controls.Add(this.btnAssociar);
            this.gbxPaciente.Controls.Add(this.txtNomePaci);
            this.gbxPaciente.Controls.Add(this.labelid);
            this.gbxPaciente.Controls.Add(this.txtId);
            this.gbxPaciente.Controls.Add(this.metroLabel6);
            this.gbxPaciente.Enabled = false;
            this.gbxPaciente.Location = new System.Drawing.Point(236, 189);
            this.gbxPaciente.Name = "gbxPaciente";
            this.gbxPaciente.Size = new System.Drawing.Size(237, 186);
            this.gbxPaciente.TabIndex = 60;
            this.gbxPaciente.TabStop = false;
            this.gbxPaciente.Text = "Informe o paciente";
            // 
            // btnAssociar
            // 
            this.btnAssociar.Location = new System.Drawing.Point(67, 128);
            this.btnAssociar.Name = "btnAssociar";
            this.btnAssociar.Size = new System.Drawing.Size(88, 45);
            this.btnAssociar.TabIndex = 48;
            this.btnAssociar.Text = "Associar";
            this.btnAssociar.UseSelectable = true;
            // 
            // txtNomePaci
            // 
            // 
            // 
            // 
            this.txtNomePaci.CustomButton.Image = null;
            this.txtNomePaci.CustomButton.Location = new System.Drawing.Point(171, 1);
            this.txtNomePaci.CustomButton.Name = "";
            this.txtNomePaci.CustomButton.Size = new System.Drawing.Size(23, 23);
            this.txtNomePaci.CustomButton.Style = MetroFramework.MetroColorStyle.Blue;
            this.txtNomePaci.CustomButton.TabIndex = 1;
            this.txtNomePaci.CustomButton.Theme = MetroFramework.MetroThemeStyle.Light;
            this.txtNomePaci.CustomButton.UseSelectable = true;
            this.txtNomePaci.CustomButton.Visible = false;
            this.txtNomePaci.Lines = new string[0];
            this.txtNomePaci.Location = new System.Drawing.Point(16, 97);
            this.txtNomePaci.MaxLength = 32767;
            this.txtNomePaci.Name = "txtNomePaci";
            this.txtNomePaci.PasswordChar = '\0';
            this.txtNomePaci.ScrollBars = System.Windows.Forms.ScrollBars.None;
            this.txtNomePaci.SelectedText = "";
            this.txtNomePaci.SelectionLength = 0;
            this.txtNomePaci.SelectionStart = 0;
            this.txtNomePaci.ShortcutsEnabled = true;
            this.txtNomePaci.Size = new System.Drawing.Size(195, 25);
            this.txtNomePaci.TabIndex = 15;
            this.txtNomePaci.UseSelectable = true;
            this.txtNomePaci.WaterMarkColor = System.Drawing.Color.FromArgb(((int)(((byte)(109)))), ((int)(((byte)(109)))), ((int)(((byte)(109)))));
            this.txtNomePaci.WaterMarkFont = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Pixel);
            // 
            // labelid
            // 
            this.labelid.AutoSize = true;
            this.labelid.Location = new System.Drawing.Point(16, 75);
            this.labelid.Name = "labelid";
            this.labelid.Size = new System.Drawing.Size(46, 19);
            this.labelid.TabIndex = 14;
            this.labelid.Text = "Nome";
            // 
            // txtId
            // 
            // 
            // 
            // 
            this.txtId.CustomButton.Image = null;
            this.txtId.CustomButton.Location = new System.Drawing.Point(61, 1);
            this.txtId.CustomButton.Name = "";
            this.txtId.CustomButton.Size = new System.Drawing.Size(23, 23);
            this.txtId.CustomButton.Style = MetroFramework.MetroColorStyle.Blue;
            this.txtId.CustomButton.TabIndex = 1;
            this.txtId.CustomButton.Theme = MetroFramework.MetroThemeStyle.Light;
            this.txtId.CustomButton.UseSelectable = true;
            this.txtId.CustomButton.Visible = false;
            this.txtId.Lines = new string[0];
            this.txtId.Location = new System.Drawing.Point(16, 38);
            this.txtId.MaxLength = 32767;
            this.txtId.Name = "txtId";
            this.txtId.PasswordChar = '\0';
            this.txtId.ScrollBars = System.Windows.Forms.ScrollBars.None;
            this.txtId.SelectedText = "";
            this.txtId.SelectionLength = 0;
            this.txtId.SelectionStart = 0;
            this.txtId.ShortcutsEnabled = true;
            this.txtId.Size = new System.Drawing.Size(85, 25);
            this.txtId.TabIndex = 13;
            this.txtId.UseSelectable = true;
            this.txtId.WaterMarkColor = System.Drawing.Color.FromArgb(((int)(((byte)(109)))), ((int)(((byte)(109)))), ((int)(((byte)(109)))));
            this.txtId.WaterMarkFont = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Pixel);
            // 
            // metroLabel6
            // 
            this.metroLabel6.AutoSize = true;
            this.metroLabel6.Location = new System.Drawing.Point(16, 16);
            this.metroLabel6.Name = "metroLabel6";
            this.metroLabel6.Size = new System.Drawing.Size(21, 19);
            this.metroLabel6.TabIndex = 12;
            this.metroLabel6.Text = "ID";
            // 
            // cbxTipo
            // 
            this.cbxTipo.FormattingEnabled = true;
            this.cbxTipo.ItemHeight = 23;
            this.cbxTipo.Items.AddRange(new object[] {
            "Visitante",
            "Familiar",
            "Colaborador"});
            this.cbxTipo.Location = new System.Drawing.Point(25, 338);
            this.cbxTipo.Name = "cbxTipo";
            this.cbxTipo.Size = new System.Drawing.Size(171, 29);
            this.cbxTipo.TabIndex = 59;
            this.cbxTipo.UseSelectable = true;
            // 
            // metroLabel5
            // 
            this.metroLabel5.AutoSize = true;
            this.metroLabel5.Location = new System.Drawing.Point(25, 316);
            this.metroLabel5.Name = "metroLabel5";
            this.metroLabel5.Size = new System.Drawing.Size(35, 19);
            this.metroLabel5.TabIndex = 58;
            this.metroLabel5.Text = "Tipo";
            // 
            // txtTelefone
            // 
            // 
            // 
            // 
            this.txtTelefone.CustomButton.Image = null;
            this.txtTelefone.CustomButton.Location = new System.Drawing.Point(147, 1);
            this.txtTelefone.CustomButton.Name = "";
            this.txtTelefone.CustomButton.Size = new System.Drawing.Size(23, 23);
            this.txtTelefone.CustomButton.Style = MetroFramework.MetroColorStyle.Blue;
            this.txtTelefone.CustomButton.TabIndex = 1;
            this.txtTelefone.CustomButton.Theme = MetroFramework.MetroThemeStyle.Light;
            this.txtTelefone.CustomButton.UseSelectable = true;
            this.txtTelefone.CustomButton.Visible = false;
            this.txtTelefone.Lines = new string[0];
            this.txtTelefone.Location = new System.Drawing.Point(25, 276);
            this.txtTelefone.MaxLength = 32767;
            this.txtTelefone.Name = "txtTelefone";
            this.txtTelefone.PasswordChar = '\0';
            this.txtTelefone.ScrollBars = System.Windows.Forms.ScrollBars.None;
            this.txtTelefone.SelectedText = "";
            this.txtTelefone.SelectionLength = 0;
            this.txtTelefone.SelectionStart = 0;
            this.txtTelefone.ShortcutsEnabled = true;
            this.txtTelefone.Size = new System.Drawing.Size(171, 25);
            this.txtTelefone.TabIndex = 57;
            this.txtTelefone.UseSelectable = true;
            this.txtTelefone.WaterMarkColor = System.Drawing.Color.FromArgb(((int)(((byte)(109)))), ((int)(((byte)(109)))), ((int)(((byte)(109)))));
            this.txtTelefone.WaterMarkFont = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Pixel);
            // 
            // metroLabel4
            // 
            this.metroLabel4.AutoSize = true;
            this.metroLabel4.Location = new System.Drawing.Point(25, 254);
            this.metroLabel4.Name = "metroLabel4";
            this.metroLabel4.Size = new System.Drawing.Size(57, 19);
            this.metroLabel4.TabIndex = 56;
            this.metroLabel4.Text = "Telefone";
            // 
            // txtEmail
            // 
            // 
            // 
            // 
            this.txtEmail.CustomButton.Image = null;
            this.txtEmail.CustomButton.Location = new System.Drawing.Point(424, 1);
            this.txtEmail.CustomButton.Name = "";
            this.txtEmail.CustomButton.Size = new System.Drawing.Size(23, 23);
            this.txtEmail.CustomButton.Style = MetroFramework.MetroColorStyle.Blue;
            this.txtEmail.CustomButton.TabIndex = 1;
            this.txtEmail.CustomButton.Theme = MetroFramework.MetroThemeStyle.Light;
            this.txtEmail.CustomButton.UseSelectable = true;
            this.txtEmail.CustomButton.Visible = false;
            this.txtEmail.Lines = new string[0];
            this.txtEmail.Location = new System.Drawing.Point(25, 152);
            this.txtEmail.MaxLength = 32767;
            this.txtEmail.Name = "txtEmail";
            this.txtEmail.PasswordChar = '\0';
            this.txtEmail.ScrollBars = System.Windows.Forms.ScrollBars.None;
            this.txtEmail.SelectedText = "";
            this.txtEmail.SelectionLength = 0;
            this.txtEmail.SelectionStart = 0;
            this.txtEmail.ShortcutsEnabled = true;
            this.txtEmail.Size = new System.Drawing.Size(448, 25);
            this.txtEmail.TabIndex = 55;
            this.txtEmail.UseSelectable = true;
            this.txtEmail.WaterMarkColor = System.Drawing.Color.FromArgb(((int)(((byte)(109)))), ((int)(((byte)(109)))), ((int)(((byte)(109)))));
            this.txtEmail.WaterMarkFont = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Pixel);
            // 
            // metroLabel3
            // 
            this.metroLabel3.AutoSize = true;
            this.metroLabel3.Location = new System.Drawing.Point(25, 130);
            this.metroLabel3.Name = "metroLabel3";
            this.metroLabel3.Size = new System.Drawing.Size(41, 19);
            this.metroLabel3.TabIndex = 54;
            this.metroLabel3.Text = "Email";
            // 
            // txtCpf
            // 
            // 
            // 
            // 
            this.txtCpf.CustomButton.Image = null;
            this.txtCpf.CustomButton.Location = new System.Drawing.Point(147, 1);
            this.txtCpf.CustomButton.Name = "";
            this.txtCpf.CustomButton.Size = new System.Drawing.Size(23, 23);
            this.txtCpf.CustomButton.Style = MetroFramework.MetroColorStyle.Blue;
            this.txtCpf.CustomButton.TabIndex = 1;
            this.txtCpf.CustomButton.Theme = MetroFramework.MetroThemeStyle.Light;
            this.txtCpf.CustomButton.UseSelectable = true;
            this.txtCpf.CustomButton.Visible = false;
            this.txtCpf.Lines = new string[0];
            this.txtCpf.Location = new System.Drawing.Point(25, 211);
            this.txtCpf.MaxLength = 32767;
            this.txtCpf.Name = "txtCpf";
            this.txtCpf.PasswordChar = '\0';
            this.txtCpf.ScrollBars = System.Windows.Forms.ScrollBars.None;
            this.txtCpf.SelectedText = "";
            this.txtCpf.SelectionLength = 0;
            this.txtCpf.SelectionStart = 0;
            this.txtCpf.ShortcutsEnabled = true;
            this.txtCpf.Size = new System.Drawing.Size(171, 25);
            this.txtCpf.TabIndex = 53;
            this.txtCpf.UseSelectable = true;
            this.txtCpf.WaterMarkColor = System.Drawing.Color.FromArgb(((int)(((byte)(109)))), ((int)(((byte)(109)))), ((int)(((byte)(109)))));
            this.txtCpf.WaterMarkFont = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Pixel);
            // 
            // metroLabel2
            // 
            this.metroLabel2.AutoSize = true;
            this.metroLabel2.Location = new System.Drawing.Point(25, 189);
            this.metroLabel2.Name = "metroLabel2";
            this.metroLabel2.Size = new System.Drawing.Size(33, 19);
            this.metroLabel2.TabIndex = 52;
            this.metroLabel2.Text = "CPF";
            // 
            // txtNome
            // 
            // 
            // 
            // 
            this.txtNome.CustomButton.Image = null;
            this.txtNome.CustomButton.Location = new System.Drawing.Point(424, 1);
            this.txtNome.CustomButton.Name = "";
            this.txtNome.CustomButton.Size = new System.Drawing.Size(23, 23);
            this.txtNome.CustomButton.Style = MetroFramework.MetroColorStyle.Blue;
            this.txtNome.CustomButton.TabIndex = 1;
            this.txtNome.CustomButton.Theme = MetroFramework.MetroThemeStyle.Light;
            this.txtNome.CustomButton.UseSelectable = true;
            this.txtNome.CustomButton.Visible = false;
            this.txtNome.Lines = new string[0];
            this.txtNome.Location = new System.Drawing.Point(25, 94);
            this.txtNome.MaxLength = 32767;
            this.txtNome.Name = "txtNome";
            this.txtNome.PasswordChar = '\0';
            this.txtNome.ScrollBars = System.Windows.Forms.ScrollBars.None;
            this.txtNome.SelectedText = "";
            this.txtNome.SelectionLength = 0;
            this.txtNome.SelectionStart = 0;
            this.txtNome.ShortcutsEnabled = true;
            this.txtNome.Size = new System.Drawing.Size(448, 25);
            this.txtNome.TabIndex = 51;
            this.txtNome.UseSelectable = true;
            this.txtNome.WaterMarkColor = System.Drawing.Color.FromArgb(((int)(((byte)(109)))), ((int)(((byte)(109)))), ((int)(((byte)(109)))));
            this.txtNome.WaterMarkFont = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Pixel);
            // 
            // metroLabel1
            // 
            this.metroLabel1.AutoSize = true;
            this.metroLabel1.Location = new System.Drawing.Point(25, 72);
            this.metroLabel1.Name = "metroLabel1";
            this.metroLabel1.Size = new System.Drawing.Size(46, 19);
            this.metroLabel1.TabIndex = 50;
            this.metroLabel1.Text = "Nome";
            // 
            // frmEditPessoa
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(496, 448);
            this.Controls.Add(this.btnCanc);
            this.Controls.Add(this.btnGravar);
            this.Controls.Add(this.gbxPaciente);
            this.Controls.Add(this.cbxTipo);
            this.Controls.Add(this.metroLabel5);
            this.Controls.Add(this.txtTelefone);
            this.Controls.Add(this.metroLabel4);
            this.Controls.Add(this.txtEmail);
            this.Controls.Add(this.metroLabel3);
            this.Controls.Add(this.txtCpf);
            this.Controls.Add(this.metroLabel2);
            this.Controls.Add(this.txtNome);
            this.Controls.Add(this.metroLabel1);
            this.Name = "frmEditPessoa";
            this.Text = "Edição de pessoa";
            this.gbxPaciente.ResumeLayout(false);
            this.gbxPaciente.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private MetroFramework.Controls.MetroButton btnCanc;
        private MetroFramework.Controls.MetroButton btnGravar;
        private System.Windows.Forms.GroupBox gbxPaciente;
        private MetroFramework.Controls.MetroButton btnAssociar;
        private MetroFramework.Controls.MetroTextBox txtNomePaci;
        private MetroFramework.Controls.MetroLabel labelid;
        private MetroFramework.Controls.MetroTextBox txtId;
        private MetroFramework.Controls.MetroLabel metroLabel6;
        private MetroFramework.Controls.MetroComboBox cbxTipo;
        private MetroFramework.Controls.MetroLabel metroLabel5;
        private MetroFramework.Controls.MetroTextBox txtTelefone;
        private MetroFramework.Controls.MetroLabel metroLabel4;
        private MetroFramework.Controls.MetroTextBox txtEmail;
        private MetroFramework.Controls.MetroLabel metroLabel3;
        private MetroFramework.Controls.MetroTextBox txtCpf;
        private MetroFramework.Controls.MetroLabel metroLabel2;
        private MetroFramework.Controls.MetroTextBox txtNome;
        private MetroFramework.Controls.MetroLabel metroLabel1;
    }
}
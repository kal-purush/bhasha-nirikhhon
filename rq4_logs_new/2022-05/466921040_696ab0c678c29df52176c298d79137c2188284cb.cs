namespace chevere_master
{
    partial class frmAdminModiUser
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
            this.panel2 = new System.Windows.Forms.Panel();
            this.btnCancelar = new System.Windows.Forms.Button();
            this.gbLista = new System.Windows.Forms.GroupBox();
            this.dgvUsuarios = new System.Windows.Forms.DataGridView();
            this.gbUserName = new System.Windows.Forms.GroupBox();
            this.txtFiltUsar = new System.Windows.Forms.TextBox();
            this.panel3 = new System.Windows.Forms.Panel();
            this.gpUser = new System.Windows.Forms.GroupBox();
            this.txtId = new System.Windows.Forms.TextBox();
            this.groupBox3 = new System.Windows.Forms.GroupBox();
            this.btn_Ver = new FontAwesome.Sharp.IconButton();
            this.txtemail = new System.Windows.Forms.TextBox();
            this.txtpassword = new System.Windows.Forms.TextBox();
            this.txtPasswordConfirmed = new System.Windows.Forms.TextBox();
            this.label11 = new System.Windows.Forms.Label();
            this.label10 = new System.Windows.Forms.Label();
            this.label9 = new System.Windows.Forms.Label();
            this.groupBox2 = new System.Windows.Forms.GroupBox();
            this.groupBox4 = new System.Windows.Forms.GroupBox();
            this.rbtTurista = new System.Windows.Forms.RadioButton();
            this.rbtAdmin = new System.Windows.Forms.RadioButton();
            this.label2 = new System.Windows.Forms.Label();
            this.cmb_Country = new System.Windows.Forms.ComboBox();
            this.rbtn_Femenino = new System.Windows.Forms.RadioButton();
            this.rbtn_Masculino = new System.Windows.Forms.RadioButton();
            this.label12 = new System.Windows.Forms.Label();
            this.cmb_departamento = new System.Windows.Forms.ComboBox();
            this.label8 = new System.Windows.Forms.Label();
            this.label7 = new System.Windows.Forms.Label();
            this.mtbnit = new System.Windows.Forms.MaskedTextBox();
            this.cmbDocument = new System.Windows.Forms.ComboBox();
            this.label3 = new System.Windows.Forms.Label();
            this.txtname = new System.Windows.Forms.TextBox();
            this.label1 = new System.Windows.Forms.Label();
            this.pnlOpciones = new System.Windows.Forms.Panel();
            this.btnModify = new FontAwesome.Sharp.IconButton();
            this.btnguardar = new FontAwesome.Sharp.IconButton();
            ((System.ComponentModel.ISupportInitialize)(this.picHome)).BeginInit();
            this.panel2.SuspendLayout();
            this.gbLista.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.dgvUsuarios)).BeginInit();
            this.gbUserName.SuspendLayout();
            this.panel3.SuspendLayout();
            this.gpUser.SuspendLayout();
            this.groupBox3.SuspendLayout();
            this.groupBox2.SuspendLayout();
            this.groupBox4.SuspendLayout();
            this.pnlOpciones.SuspendLayout();
            this.SuspendLayout();
            // 
            // picHome
            // 
            this.picHome.Dock = System.Windows.Forms.DockStyle.None;
            this.picHome.Location = new System.Drawing.Point(1020, 318);
            this.picHome.Size = new System.Drawing.Size(102, 253);
            // 
            // lbl_init_text
            // 
            this.lbl_init_text.Location = new System.Drawing.Point(279, 171);
            this.lbl_init_text.Size = new System.Drawing.Size(93, 101);
            // 
            // panel2
            // 
            this.panel2.Controls.Add(this.btnCancelar);
            this.panel2.Controls.Add(this.gbLista);
            this.panel2.Controls.Add(this.gbUserName);
            this.panel2.Dock = System.Windows.Forms.DockStyle.Left;
            this.panel2.Location = new System.Drawing.Point(0, 0);
            this.panel2.Name = "panel2";
            this.panel2.Size = new System.Drawing.Size(313, 697);
            this.panel2.TabIndex = 0;
            // 
            // btnCancelar
            // 
            this.btnCancelar.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.btnCancelar.BackColor = System.Drawing.Color.Red;
            this.btnCancelar.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.btnCancelar.Font = new System.Drawing.Font("Century Gothic", 10.2F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnCancelar.ForeColor = System.Drawing.Color.White;
            this.btnCancelar.Location = new System.Drawing.Point(25, 548);
            this.btnCancelar.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.btnCancelar.Name = "btnCancelar";
            this.btnCancelar.Size = new System.Drawing.Size(252, 32);
            this.btnCancelar.TabIndex = 18;
            this.btnCancelar.Text = "Cancelar";
            this.btnCancelar.UseVisualStyleBackColor = false;
            this.btnCancelar.Click += new System.EventHandler(this.btnCancelar_Click);
            // 
            // gbLista
            // 
            this.gbLista.Controls.Add(this.dgvUsuarios);
            this.gbLista.Font = new System.Drawing.Font("Microsoft Sans Serif", 10.2F);
            this.gbLista.Location = new System.Drawing.Point(22, 135);
            this.gbLista.Name = "gbLista";
            this.gbLista.Size = new System.Drawing.Size(255, 352);
            this.gbLista.TabIndex = 0;
            this.gbLista.TabStop = false;
            this.gbLista.Text = "Lista de Usuarios";
            // 
            // dgvUsuarios
            // 
            this.dgvUsuarios.AutoSizeColumnsMode = System.Windows.Forms.DataGridViewAutoSizeColumnsMode.Fill;
            this.dgvUsuarios.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dgvUsuarios.Dock = System.Windows.Forms.DockStyle.Fill;
            this.dgvUsuarios.Location = new System.Drawing.Point(3, 23);
            this.dgvUsuarios.Name = "dgvUsuarios";
            this.dgvUsuarios.RowHeadersWidth = 51;
            this.dgvUsuarios.RowTemplate.Height = 24;
            this.dgvUsuarios.SelectionMode = System.Windows.Forms.DataGridViewSelectionMode.FullRowSelect;
            this.dgvUsuarios.Size = new System.Drawing.Size(249, 326);
            this.dgvUsuarios.TabIndex = 0;
            this.dgvUsuarios.CellContentClick += new System.Windows.Forms.DataGridViewCellEventHandler(this.dgvUsuarios_CellContentClick);
            this.dgvUsuarios.DoubleClick += new System.EventHandler(this.dgvUsuarios_DoubleClick);
            // 
            // gbUserName
            // 
            this.gbUserName.Controls.Add(this.txtFiltUsar);
            this.gbUserName.Font = new System.Drawing.Font("Microsoft Sans Serif", 10.2F);
            this.gbUserName.Location = new System.Drawing.Point(19, 35);
            this.gbUserName.Name = "gbUserName";
            this.gbUserName.Size = new System.Drawing.Size(258, 71);
            this.gbUserName.TabIndex = 0;
            this.gbUserName.TabStop = false;
            this.gbUserName.Text = "Ingrese el nombre de Usuario";
            // 
            // txtFiltUsar
            // 
            this.txtFiltUsar.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.txtFiltUsar.Location = new System.Drawing.Point(3, 41);
            this.txtFiltUsar.Name = "txtFiltUsar";
            this.txtFiltUsar.Size = new System.Drawing.Size(252, 27);
            this.txtFiltUsar.TabIndex = 0;
            this.txtFiltUsar.KeyUp += new System.Windows.Forms.KeyEventHandler(this.txtFiltUsar_KeyUp);
            // 
            // panel3
            // 
            this.panel3.Controls.Add(this.gpUser);
            this.panel3.Controls.Add(this.pnlOpciones);
            this.panel3.Dock = System.Windows.Forms.DockStyle.Right;
            this.panel3.Location = new System.Drawing.Point(313, 0);
            this.panel3.Name = "panel3";
            this.panel3.Size = new System.Drawing.Size(769, 697);
            this.panel3.TabIndex = 0;
            // 
            // gpUser
            // 
            this.gpUser.Controls.Add(this.txtId);
            this.gpUser.Controls.Add(this.groupBox3);
            this.gpUser.Controls.Add(this.groupBox2);
            this.gpUser.Controls.Add(this.label1);
            this.gpUser.Font = new System.Drawing.Font("Microsoft Sans Serif", 10.2F);
            this.gpUser.Location = new System.Drawing.Point(23, 25);
            this.gpUser.Name = "gpUser";
            this.gpUser.Size = new System.Drawing.Size(717, 601);
            this.gpUser.TabIndex = 18;
            this.gpUser.TabStop = false;
            this.gpUser.Text = "Datos de Usuario";
            // 
            // txtId
            // 
            this.txtId.Location = new System.Drawing.Point(80, 48);
            this.txtId.Name = "txtId";
            this.txtId.Size = new System.Drawing.Size(191, 27);
            this.txtId.TabIndex = 31;
            // 
            // groupBox3
            // 
            this.groupBox3.Controls.Add(this.btn_Ver);
            this.groupBox3.Controls.Add(this.txtemail);
            this.groupBox3.Controls.Add(this.txtpassword);
            this.groupBox3.Controls.Add(this.txtPasswordConfirmed);
            this.groupBox3.Controls.Add(this.label11);
            this.groupBox3.Controls.Add(this.label10);
            this.groupBox3.Controls.Add(this.label9);
            this.groupBox3.Font = new System.Drawing.Font("Microsoft Sans Serif", 10.2F);
            this.groupBox3.Location = new System.Drawing.Point(31, 407);
            this.groupBox3.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.groupBox3.Name = "groupBox3";
            this.groupBox3.Padding = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.groupBox3.Size = new System.Drawing.Size(658, 167);
            this.groupBox3.TabIndex = 30;
            this.groupBox3.TabStop = false;
            this.groupBox3.Text = "Información de inicio de sesión";
            // 
            // btn_Ver
            // 
            this.btn_Ver.FlatAppearance.BorderSize = 0;
            this.btn_Ver.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btn_Ver.IconChar = FontAwesome.Sharp.IconChar.Eye;
            this.btn_Ver.IconColor = System.Drawing.Color.Black;
            this.btn_Ver.IconFont = FontAwesome.Sharp.IconFont.Auto;
            this.btn_Ver.IconSize = 30;
            this.btn_Ver.ImageAlign = System.Drawing.ContentAlignment.TopCenter;
            this.btn_Ver.Location = new System.Drawing.Point(365, 70);
            this.btn_Ver.Margin = new System.Windows.Forms.Padding(0);
            this.btn_Ver.Name = "btn_Ver";
            this.btn_Ver.Size = new System.Drawing.Size(56, 37);
            this.btn_Ver.TabIndex = 14;
            this.btn_Ver.UseVisualStyleBackColor = true;
            this.btn_Ver.Click += new System.EventHandler(this.btn_Ver_Click);
            // 
            // txtemail
            // 
            this.txtemail.Location = new System.Drawing.Point(301, 34);
            this.txtemail.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.txtemail.Name = "txtemail";
            this.txtemail.Size = new System.Drawing.Size(341, 27);
            this.txtemail.TabIndex = 5;
            // 
            // txtpassword
            // 
            this.txtpassword.Location = new System.Drawing.Point(423, 75);
            this.txtpassword.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.txtpassword.Name = "txtpassword";
            this.txtpassword.Size = new System.Drawing.Size(220, 27);
            this.txtpassword.TabIndex = 4;
            // 
            // txtPasswordConfirmed
            // 
            this.txtPasswordConfirmed.Location = new System.Drawing.Point(423, 116);
            this.txtPasswordConfirmed.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.txtPasswordConfirmed.Name = "txtPasswordConfirmed";
            this.txtPasswordConfirmed.Size = new System.Drawing.Size(220, 27);
            this.txtPasswordConfirmed.TabIndex = 3;
            // 
            // label11
            // 
            this.label11.AutoSize = true;
            this.label11.Location = new System.Drawing.Point(61, 116);
            this.label11.Name = "label11";
            this.label11.Size = new System.Drawing.Size(179, 20);
            this.label11.TabIndex = 2;
            this.label11.Text = "Confirmar Contraseña:";
            // 
            // label10
            // 
            this.label10.AutoSize = true;
            this.label10.Location = new System.Drawing.Point(61, 75);
            this.label10.Name = "label10";
            this.label10.Size = new System.Drawing.Size(100, 20);
            this.label10.TabIndex = 1;
            this.label10.Text = "Contraseña:";
            // 
            // label9
            // 
            this.label9.AutoSize = true;
            this.label9.Location = new System.Drawing.Point(61, 34);
            this.label9.Name = "label9";
            this.label9.Size = new System.Drawing.Size(65, 20);
            this.label9.TabIndex = 0;
            this.label9.Text = "Correo:";
            // 
            // groupBox2
            // 
            this.groupBox2.Controls.Add(this.groupBox4);
            this.groupBox2.Controls.Add(this.label2);
            this.groupBox2.Controls.Add(this.cmb_Country);
            this.groupBox2.Controls.Add(this.rbtn_Femenino);
            this.groupBox2.Controls.Add(this.rbtn_Masculino);
            this.groupBox2.Controls.Add(this.label12);
            this.groupBox2.Controls.Add(this.cmb_departamento);
            this.groupBox2.Controls.Add(this.label8);
            this.groupBox2.Controls.Add(this.label7);
            this.groupBox2.Controls.Add(this.mtbnit);
            this.groupBox2.Controls.Add(this.cmbDocument);
            this.groupBox2.Controls.Add(this.label3);
            this.groupBox2.Controls.Add(this.txtname);
            this.groupBox2.Font = new System.Drawing.Font("Microsoft Sans Serif", 10.2F);
            this.groupBox2.Location = new System.Drawing.Point(31, 110);
            this.groupBox2.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.groupBox2.Name = "groupBox2";
            this.groupBox2.Padding = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.groupBox2.Size = new System.Drawing.Size(658, 257);
            this.groupBox2.TabIndex = 27;
            this.groupBox2.TabStop = false;
            this.groupBox2.Text = "Informacion Personal";
            // 
            // groupBox4
            // 
            this.groupBox4.Controls.Add(this.rbtTurista);
            this.groupBox4.Controls.Add(this.rbtAdmin);
            this.groupBox4.Location = new System.Drawing.Point(438, 103);
            this.groupBox4.Name = "groupBox4";
            this.groupBox4.Size = new System.Drawing.Size(205, 141);
            this.groupBox4.TabIndex = 31;
            this.groupBox4.TabStop = false;
            this.groupBox4.Text = "Tipo de Usuario";
            // 
            // rbtTurista
            // 
            this.rbtTurista.AutoSize = true;
            this.rbtTurista.Location = new System.Drawing.Point(6, 76);
            this.rbtTurista.Name = "rbtTurista";
            this.rbtTurista.Size = new System.Drawing.Size(145, 24);
            this.rbtTurista.TabIndex = 1;
            this.rbtTurista.TabStop = true;
            this.rbtTurista.Text = "Usuario Turista";
            this.rbtTurista.UseVisualStyleBackColor = true;
            // 
            // rbtAdmin
            // 
            this.rbtAdmin.AutoSize = true;
            this.rbtAdmin.Location = new System.Drawing.Point(7, 34);
            this.rbtAdmin.Name = "rbtAdmin";
            this.rbtAdmin.Size = new System.Drawing.Size(197, 24);
            this.rbtAdmin.TabIndex = 0;
            this.rbtAdmin.TabStop = true;
            this.rbtAdmin.Text = "Usuario Administrador";
            this.rbtAdmin.UseVisualStyleBackColor = true;
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(13, 100);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(100, 20);
            this.label2.TabIndex = 1;
            this.label2.Text = "Documento:";
            // 
            // cmb_Country
            // 
            this.cmb_Country.FormattingEnabled = true;
            this.cmb_Country.Items.AddRange(new object[] {
            " "});
            this.cmb_Country.Location = new System.Drawing.Point(254, 216);
            this.cmb_Country.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.cmb_Country.Name = "cmb_Country";
            this.cmb_Country.Size = new System.Drawing.Size(171, 28);
            this.cmb_Country.TabIndex = 41;
            // 
            // rbtn_Femenino
            // 
            this.rbtn_Femenino.AutoSize = true;
            this.rbtn_Femenino.Location = new System.Drawing.Point(157, 209);
            this.rbtn_Femenino.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.rbtn_Femenino.Name = "rbtn_Femenino";
            this.rbtn_Femenino.Size = new System.Drawing.Size(40, 24);
            this.rbtn_Femenino.TabIndex = 40;
            this.rbtn_Femenino.TabStop = true;
            this.rbtn_Femenino.Text = "F";
            this.rbtn_Femenino.UseVisualStyleBackColor = true;
            // 
            // rbtn_Masculino
            // 
            this.rbtn_Masculino.AutoSize = true;
            this.rbtn_Masculino.Location = new System.Drawing.Point(107, 209);
            this.rbtn_Masculino.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.rbtn_Masculino.Name = "rbtn_Masculino";
            this.rbtn_Masculino.Size = new System.Drawing.Size(44, 24);
            this.rbtn_Masculino.TabIndex = 39;
            this.rbtn_Masculino.TabStop = true;
            this.rbtn_Masculino.Text = "M";
            this.rbtn_Masculino.UseVisualStyleBackColor = true;
            // 
            // label12
            // 
            this.label12.AutoSize = true;
            this.label12.Location = new System.Drawing.Point(248, 183);
            this.label12.Name = "label12";
            this.label12.Size = new System.Drawing.Size(116, 20);
            this.label12.TabIndex = 38;
            this.label12.Text = "Nacionalidad:*";
            // 
            // cmb_departamento
            // 
            this.cmb_departamento.FormattingEnabled = true;
            this.cmb_departamento.Items.AddRange(new object[] {
            " "});
            this.cmb_departamento.Location = new System.Drawing.Point(253, 133);
            this.cmb_departamento.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.cmb_departamento.Name = "cmb_departamento";
            this.cmb_departamento.Size = new System.Drawing.Size(171, 28);
            this.cmb_departamento.TabIndex = 36;
            // 
            // label8
            // 
            this.label8.AutoSize = true;
            this.label8.Location = new System.Drawing.Point(13, 212);
            this.label8.Name = "label8";
            this.label8.Size = new System.Drawing.Size(69, 20);
            this.label8.TabIndex = 34;
            this.label8.Text = "Genero:";
            // 
            // label7
            // 
            this.label7.AutoSize = true;
            this.label7.Location = new System.Drawing.Point(248, 100);
            this.label7.Name = "label7";
            this.label7.Size = new System.Drawing.Size(177, 20);
            this.label7.TabIndex = 33;
            this.label7.Text = "Departamento Reside:";
            // 
            // mtbnit
            // 
            this.mtbnit.Location = new System.Drawing.Point(111, 133);
            this.mtbnit.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.mtbnit.Name = "mtbnit";
            this.mtbnit.Size = new System.Drawing.Size(127, 27);
            this.mtbnit.TabIndex = 30;
            // 
            // cmbDocument
            // 
            this.cmbDocument.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.cmbDocument.FormattingEnabled = true;
            this.cmbDocument.Items.AddRange(new object[] {
            "DUI",
            "NIT"});
            this.cmbDocument.Location = new System.Drawing.Point(19, 133);
            this.cmbDocument.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.cmbDocument.Name = "cmbDocument";
            this.cmbDocument.Size = new System.Drawing.Size(79, 28);
            this.cmbDocument.TabIndex = 28;
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(13, 27);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(159, 20);
            this.label3.TabIndex = 26;
            this.label3.Text = "Nombre de Usuario:";
            // 
            // txtname
            // 
            this.txtname.Location = new System.Drawing.Point(19, 54);
            this.txtname.Margin = new System.Windows.Forms.Padding(4);
            this.txtname.Name = "txtname";
            this.txtname.Size = new System.Drawing.Size(220, 27);
            this.txtname.TabIndex = 6;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(43, 48);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(31, 20);
            this.label1.TabIndex = 0;
            this.label1.Text = "ID:";
            // 
            // pnlOpciones
            // 
            this.pnlOpciones.Controls.Add(this.btnModify);
            this.pnlOpciones.Controls.Add(this.btnguardar);
            this.pnlOpciones.Location = new System.Drawing.Point(330, 632);
            this.pnlOpciones.Name = "pnlOpciones";
            this.pnlOpciones.Size = new System.Drawing.Size(410, 53);
            this.pnlOpciones.TabIndex = 19;
            // 
            // btnModify
            // 
            this.btnModify.IconChar = FontAwesome.Sharp.IconChar.Tools;
            this.btnModify.IconColor = System.Drawing.Color.Black;
            this.btnModify.IconFont = FontAwesome.Sharp.IconFont.Auto;
            this.btnModify.Location = new System.Drawing.Point(279, 3);
            this.btnModify.Name = "btnModify";
            this.btnModify.Size = new System.Drawing.Size(127, 47);
            this.btnModify.TabIndex = 34;
            this.btnModify.Text = "Modify";
            this.btnModify.TextImageRelation = System.Windows.Forms.TextImageRelation.ImageBeforeText;
            this.btnModify.UseVisualStyleBackColor = true;
            this.btnModify.Click += new System.EventHandler(this.btnModify_Click);
            // 
            // btnguardar
            // 
            this.btnguardar.IconChar = FontAwesome.Sharp.IconChar.Save;
            this.btnguardar.IconColor = System.Drawing.Color.Black;
            this.btnguardar.IconFont = FontAwesome.Sharp.IconFont.Auto;
            this.btnguardar.Location = new System.Drawing.Point(147, 2);
            this.btnguardar.Name = "btnguardar";
            this.btnguardar.Size = new System.Drawing.Size(127, 49);
            this.btnguardar.TabIndex = 19;
            this.btnguardar.Text = "Guardar";
            this.btnguardar.TextImageRelation = System.Windows.Forms.TextImageRelation.ImageBeforeText;
            this.btnguardar.UseVisualStyleBackColor = true;
            this.btnguardar.Click += new System.EventHandler(this.btnguardar_Click_1);
            // 
            // frmAdminModiUser
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1082, 697);
            this.Controls.Add(this.panel2);
            this.Controls.Add(this.panel3);
            this.Name = "frmAdminModiUser";
            this.Text = "frmAdminModiUser";
            this.Controls.SetChildIndex(this.picHome, 0);
            this.Controls.SetChildIndex(this.lbl_init_text, 0);
            this.Controls.SetChildIndex(this.panel3, 0);
            this.Controls.SetChildIndex(this.panel2, 0);
            ((System.ComponentModel.ISupportInitialize)(this.picHome)).EndInit();
            this.panel2.ResumeLayout(false);
            this.gbLista.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.dgvUsuarios)).EndInit();
            this.gbUserName.ResumeLayout(false);
            this.gbUserName.PerformLayout();
            this.panel3.ResumeLayout(false);
            this.gpUser.ResumeLayout(false);
            this.gpUser.PerformLayout();
            this.groupBox3.ResumeLayout(false);
            this.groupBox3.PerformLayout();
            this.groupBox2.ResumeLayout(false);
            this.groupBox2.PerformLayout();
            this.groupBox4.ResumeLayout(false);
            this.groupBox4.PerformLayout();
            this.pnlOpciones.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion
        private System.Windows.Forms.Panel panel2;
        private System.Windows.Forms.Panel panel3;
        private System.Windows.Forms.GroupBox gbLista;
        private System.Windows.Forms.DataGridView dgvUsuarios;
        private System.Windows.Forms.GroupBox gbUserName;
        private System.Windows.Forms.TextBox txtFiltUsar;
        private System.Windows.Forms.GroupBox gpUser;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.GroupBox groupBox2;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.ComboBox cmb_Country;
        private System.Windows.Forms.RadioButton rbtn_Femenino;
        private System.Windows.Forms.RadioButton rbtn_Masculino;
        private System.Windows.Forms.Label label12;
        private System.Windows.Forms.ComboBox cmb_departamento;
        private System.Windows.Forms.Label label8;
        private System.Windows.Forms.Label label7;
        private System.Windows.Forms.MaskedTextBox mtbnit;
        private System.Windows.Forms.ComboBox cmbDocument;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.TextBox txtname;
        private System.Windows.Forms.GroupBox groupBox3;
        private System.Windows.Forms.TextBox txtemail;
        private System.Windows.Forms.TextBox txtpassword;
        private System.Windows.Forms.TextBox txtPasswordConfirmed;
        private System.Windows.Forms.Label label11;
        private System.Windows.Forms.Label label10;
        private System.Windows.Forms.Label label9;
        protected System.Windows.Forms.Button btnCancelar;
        private FontAwesome.Sharp.IconButton btnModify;
        private System.Windows.Forms.TextBox txtId;
        private System.Windows.Forms.GroupBox groupBox4;
        private System.Windows.Forms.RadioButton rbtTurista;
        private System.Windows.Forms.RadioButton rbtAdmin;
        private FontAwesome.Sharp.IconButton btnguardar;
        private System.Windows.Forms.Panel pnlOpciones;
        private FontAwesome.Sharp.IconButton btn_Ver;
    }
}
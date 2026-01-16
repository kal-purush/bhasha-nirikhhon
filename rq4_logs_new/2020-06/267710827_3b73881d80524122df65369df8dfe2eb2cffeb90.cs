namespace chatbottest1
{
    partial class Form_AddPrograms_Boss
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
            this.lbl_add_program = new System.Windows.Forms.Label();
            this.txt_Id_Programa = new System.Windows.Forms.TextBox();
            this.idCompania = new System.Windows.Forms.TextBox();
            this.datoErroneoProgram = new System.Windows.Forms.Label();
            this.datoErroneoCompania = new System.Windows.Forms.Label();
            this.nameProgram = new System.Windows.Forms.TextBox();
            this.datoErroneoNombrePrograma = new System.Windows.Forms.Label();
            this.datoErroneoTipoPrograma = new System.Windows.Forms.Label();
            this.tipoPrograma = new System.Windows.Forms.TextBox();
            this.datoErroneoFechaInicio = new System.Windows.Forms.Label();
            this.fechaInicio = new System.Windows.Forms.Label();
            this.pick_fecha_inicioPrograma = new System.Windows.Forms.DateTimePicker();
            this.backHome = new System.Windows.Forms.Button();
            this.bt_adduser = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // lbl_add_program
            // 
            this.lbl_add_program.AutoSize = true;
            this.lbl_add_program.Font = new System.Drawing.Font("Century Gothic", 31.8F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lbl_add_program.ForeColor = System.Drawing.Color.DimGray;
            this.lbl_add_program.Location = new System.Drawing.Point(20, 26);
            this.lbl_add_program.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lbl_add_program.Name = "lbl_add_program";
            this.lbl_add_program.Size = new System.Drawing.Size(550, 63);
            this.lbl_add_program.TabIndex = 8;
            this.lbl_add_program.Text = "AÑADIR PROGRAMA";
            // 
            // txt_Id_Programa
            // 
            this.txt_Id_Programa.BackColor = System.Drawing.Color.White;
            this.txt_Id_Programa.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.txt_Id_Programa.Font = new System.Drawing.Font("Century Gothic", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.txt_Id_Programa.ForeColor = System.Drawing.Color.DimGray;
            this.txt_Id_Programa.Location = new System.Drawing.Point(12, 139);
            this.txt_Id_Programa.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.txt_Id_Programa.Name = "txt_Id_Programa";
            this.txt_Id_Programa.Size = new System.Drawing.Size(266, 25);
            this.txt_Id_Programa.TabIndex = 43;
            this.txt_Id_Programa.Text = "ID PROGRAMA";
            this.txt_Id_Programa.TextAlign = System.Windows.Forms.HorizontalAlignment.Center;
            // 
            // idCompania
            // 
            this.idCompania.BackColor = System.Drawing.Color.White;
            this.idCompania.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.idCompania.Font = new System.Drawing.Font("Century Gothic", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.idCompania.ForeColor = System.Drawing.Color.DimGray;
            this.idCompania.Location = new System.Drawing.Point(335, 139);
            this.idCompania.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.idCompania.Name = "idCompania";
            this.idCompania.Size = new System.Drawing.Size(266, 25);
            this.idCompania.TabIndex = 44;
            this.idCompania.Text = "ID COMPAÑIA";
            this.idCompania.TextAlign = System.Windows.Forms.HorizontalAlignment.Center;
            // 
            // datoErroneoProgram
            // 
            this.datoErroneoProgram.AutoSize = true;
            this.datoErroneoProgram.Font = new System.Drawing.Font("Century Gothic", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.datoErroneoProgram.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(27)))), ((int)(((byte)(131)))), ((int)(((byte)(185)))));
            this.datoErroneoProgram.Location = new System.Drawing.Point(12, 166);
            this.datoErroneoProgram.Name = "datoErroneoProgram";
            this.datoErroneoProgram.Size = new System.Drawing.Size(108, 20);
            this.datoErroneoProgram.TabIndex = 45;
            this.datoErroneoProgram.Text = "Dato erróneo";
            this.datoErroneoProgram.Visible = false;
            // 
            // datoErroneoCompania
            // 
            this.datoErroneoCompania.AutoSize = true;
            this.datoErroneoCompania.Font = new System.Drawing.Font("Century Gothic", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.datoErroneoCompania.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(27)))), ((int)(((byte)(131)))), ((int)(((byte)(185)))));
            this.datoErroneoCompania.Location = new System.Drawing.Point(331, 166);
            this.datoErroneoCompania.Name = "datoErroneoCompania";
            this.datoErroneoCompania.Size = new System.Drawing.Size(108, 20);
            this.datoErroneoCompania.TabIndex = 46;
            this.datoErroneoCompania.Text = "Dato erróneo";
            this.datoErroneoCompania.Visible = false;
            // 
            // nameProgram
            // 
            this.nameProgram.BackColor = System.Drawing.Color.White;
            this.nameProgram.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.nameProgram.Font = new System.Drawing.Font("Century Gothic", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.nameProgram.ForeColor = System.Drawing.Color.DimGray;
            this.nameProgram.Location = new System.Drawing.Point(45, 246);
            this.nameProgram.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.nameProgram.Name = "nameProgram";
            this.nameProgram.Size = new System.Drawing.Size(511, 25);
            this.nameProgram.TabIndex = 47;
            this.nameProgram.Text = "NOMBRE DEL PROGRAMA";
            this.nameProgram.TextAlign = System.Windows.Forms.HorizontalAlignment.Center;
            // 
            // datoErroneoNombrePrograma
            // 
            this.datoErroneoNombrePrograma.AutoSize = true;
            this.datoErroneoNombrePrograma.Font = new System.Drawing.Font("Century Gothic", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.datoErroneoNombrePrograma.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(27)))), ((int)(((byte)(131)))), ((int)(((byte)(185)))));
            this.datoErroneoNombrePrograma.Location = new System.Drawing.Point(249, 273);
            this.datoErroneoNombrePrograma.Name = "datoErroneoNombrePrograma";
            this.datoErroneoNombrePrograma.Size = new System.Drawing.Size(108, 20);
            this.datoErroneoNombrePrograma.TabIndex = 48;
            this.datoErroneoNombrePrograma.Text = "Dato erróneo";
            this.datoErroneoNombrePrograma.Visible = false;
            // 
            // datoErroneoTipoPrograma
            // 
            this.datoErroneoTipoPrograma.AutoSize = true;
            this.datoErroneoTipoPrograma.Font = new System.Drawing.Font("Century Gothic", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.datoErroneoTipoPrograma.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(27)))), ((int)(((byte)(131)))), ((int)(((byte)(185)))));
            this.datoErroneoTipoPrograma.Location = new System.Drawing.Point(169, 484);
            this.datoErroneoTipoPrograma.Name = "datoErroneoTipoPrograma";
            this.datoErroneoTipoPrograma.Size = new System.Drawing.Size(108, 20);
            this.datoErroneoTipoPrograma.TabIndex = 52;
            this.datoErroneoTipoPrograma.Text = "Dato erróneo";
            this.datoErroneoTipoPrograma.Visible = false;
            // 
            // tipoPrograma
            // 
            this.tipoPrograma.BackColor = System.Drawing.Color.White;
            this.tipoPrograma.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.tipoPrograma.Font = new System.Drawing.Font("Century Gothic", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.tipoPrograma.ForeColor = System.Drawing.Color.DimGray;
            this.tipoPrograma.Location = new System.Drawing.Point(173, 457);
            this.tipoPrograma.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.tipoPrograma.Name = "tipoPrograma";
            this.tipoPrograma.Size = new System.Drawing.Size(266, 25);
            this.tipoPrograma.TabIndex = 50;
            this.tipoPrograma.Text = "TIPO DE PROGRAMA";
            this.tipoPrograma.TextAlign = System.Windows.Forms.HorizontalAlignment.Center;
            // 
            // datoErroneoFechaInicio
            // 
            this.datoErroneoFechaInicio.AutoSize = true;
            this.datoErroneoFechaInicio.Font = new System.Drawing.Font("Century Gothic", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.datoErroneoFechaInicio.ForeColor = System.Drawing.Color.FromArgb(((int)(((byte)(27)))), ((int)(((byte)(131)))), ((int)(((byte)(185)))));
            this.datoErroneoFechaInicio.Location = new System.Drawing.Point(57, 376);
            this.datoErroneoFechaInicio.Name = "datoErroneoFechaInicio";
            this.datoErroneoFechaInicio.Size = new System.Drawing.Size(108, 20);
            this.datoErroneoFechaInicio.TabIndex = 53;
            this.datoErroneoFechaInicio.Text = "Dato erróneo";
            this.datoErroneoFechaInicio.Visible = false;
            // 
            // fechaInicio
            // 
            this.fechaInicio.AutoSize = true;
            this.fechaInicio.Font = new System.Drawing.Font("Century Gothic", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.fechaInicio.ForeColor = System.Drawing.Color.DimGray;
            this.fechaInicio.Location = new System.Drawing.Point(57, 353);
            this.fechaInicio.Name = "fechaInicio";
            this.fechaInicio.Size = new System.Drawing.Size(177, 23);
            this.fechaInicio.TabIndex = 54;
            this.fechaInicio.Text = "FECHA DE INICIO";
            // 
            // pick_fecha_inicioPrograma
            // 
            this.pick_fecha_inicioPrograma.CustomFormat = "dd-MM-yyyy";
            this.pick_fecha_inicioPrograma.Font = new System.Drawing.Font("Century Gothic", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.pick_fecha_inicioPrograma.Format = System.Windows.Forms.DateTimePickerFormat.Custom;
            this.pick_fecha_inicioPrograma.Location = new System.Drawing.Point(335, 344);
            this.pick_fecha_inicioPrograma.MinDate = new System.DateTime(1920, 1, 1, 0, 0, 0, 0);
            this.pick_fecha_inicioPrograma.Name = "pick_fecha_inicioPrograma";
            this.pick_fecha_inicioPrograma.Size = new System.Drawing.Size(219, 32);
            this.pick_fecha_inicioPrograma.TabIndex = 55;
            this.pick_fecha_inicioPrograma.Value = new System.DateTime(2000, 1, 1, 0, 0, 0, 0);
            // 
            // backHome
            // 
            this.backHome.BackColor = System.Drawing.SystemColors.ControlDarkDark;
            this.backHome.FlatAppearance.BorderSize = 0;
            this.backHome.FlatAppearance.MouseDownBackColor = System.Drawing.Color.FromArgb(((int)(((byte)(28)))), ((int)(((byte)(28)))), ((int)(((byte)(28)))));
            this.backHome.FlatAppearance.MouseOverBackColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.backHome.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.backHome.Font = new System.Drawing.Font("Microsoft Sans Serif", 11F);
            this.backHome.ForeColor = System.Drawing.Color.LightGray;
            this.backHome.Location = new System.Drawing.Point(106, 556);
            this.backHome.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.backHome.Name = "backHome";
            this.backHome.Size = new System.Drawing.Size(101, 33);
            this.backHome.TabIndex = 57;
            this.backHome.Text = "VOLVER";
            this.backHome.UseVisualStyleBackColor = false;
            this.backHome.Click += new System.EventHandler(this.backHome_Click);
            // 
            // bt_adduser
            // 
            this.bt_adduser.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(27)))), ((int)(((byte)(131)))), ((int)(((byte)(185)))));
            this.bt_adduser.FlatAppearance.BorderSize = 0;
            this.bt_adduser.FlatAppearance.MouseDownBackColor = System.Drawing.Color.FromArgb(((int)(((byte)(28)))), ((int)(((byte)(28)))), ((int)(((byte)(28)))));
            this.bt_adduser.FlatAppearance.MouseOverBackColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.bt_adduser.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.bt_adduser.Font = new System.Drawing.Font("Microsoft Sans Serif", 11F);
            this.bt_adduser.ForeColor = System.Drawing.Color.LightGray;
            this.bt_adduser.Location = new System.Drawing.Point(281, 556);
            this.bt_adduser.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.bt_adduser.Name = "bt_adduser";
            this.bt_adduser.Size = new System.Drawing.Size(232, 33);
            this.bt_adduser.TabIndex = 56;
            this.bt_adduser.Text = "AÑADIR";
            this.bt_adduser.UseVisualStyleBackColor = false;
            this.bt_adduser.Click += new System.EventHandler(this.bt_adduser_Click);
            // 
            // Form_AddPrograms_Boss
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(615, 615);
            this.Controls.Add(this.backHome);
            this.Controls.Add(this.bt_adduser);
            this.Controls.Add(this.pick_fecha_inicioPrograma);
            this.Controls.Add(this.fechaInicio);
            this.Controls.Add(this.datoErroneoFechaInicio);
            this.Controls.Add(this.datoErroneoTipoPrograma);
            this.Controls.Add(this.tipoPrograma);
            this.Controls.Add(this.datoErroneoNombrePrograma);
            this.Controls.Add(this.nameProgram);
            this.Controls.Add(this.datoErroneoCompania);
            this.Controls.Add(this.datoErroneoProgram);
            this.Controls.Add(this.idCompania);
            this.Controls.Add(this.txt_Id_Programa);
            this.Controls.Add(this.lbl_add_program);
            this.MaximumSize = new System.Drawing.Size(633, 662);
            this.MinimumSize = new System.Drawing.Size(633, 662);
            this.Name = "Form_AddPrograms_Boss";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Formulario añadir programa";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label lbl_add_program;
        private System.Windows.Forms.TextBox txt_Id_Programa;
        private System.Windows.Forms.TextBox idCompania;
        private System.Windows.Forms.Label datoErroneoProgram;
        private System.Windows.Forms.Label datoErroneoCompania;
        private System.Windows.Forms.TextBox nameProgram;
        private System.Windows.Forms.Label datoErroneoNombrePrograma;
        private System.Windows.Forms.Label datoErroneoTipoPrograma;
        private System.Windows.Forms.TextBox tipoPrograma;
        private System.Windows.Forms.Label datoErroneoFechaInicio;
        private System.Windows.Forms.Label fechaInicio;
        private System.Windows.Forms.DateTimePicker pick_fecha_inicioPrograma;
        private System.Windows.Forms.Button backHome;
        private System.Windows.Forms.Button bt_adduser;
    }
}
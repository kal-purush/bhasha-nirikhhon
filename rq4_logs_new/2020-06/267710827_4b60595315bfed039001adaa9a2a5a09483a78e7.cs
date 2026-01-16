namespace chatbottest1
{
    partial class Form_add_event
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(Form_add_event));
            this.lbl_addUser = new System.Windows.Forms.Label();
            this.fecha_evento = new System.Windows.Forms.Label();
            this.lbl_asunto = new System.Windows.Forms.Label();
            this.txt_asunto = new System.Windows.Forms.TextBox();
            this.pick_fecha_evento = new System.Windows.Forms.DateTimePicker();
            this.lbl_participantes = new System.Windows.Forms.Label();
            this.panel_users = new System.Windows.Forms.Panel();
            this.comboBox1 = new System.Windows.Forms.ComboBox();
            this.bt_adduser = new System.Windows.Forms.Button();
            this.backHome = new System.Windows.Forms.Button();
            this.bt_add_event = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // lbl_addUser
            // 
            this.lbl_addUser.AutoSize = true;
            this.lbl_addUser.Font = new System.Drawing.Font("Century Gothic", 31.8F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lbl_addUser.ForeColor = System.Drawing.Color.DimGray;
            this.lbl_addUser.Location = new System.Drawing.Point(53, 19);
            this.lbl_addUser.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lbl_addUser.Name = "lbl_addUser";
            this.lbl_addUser.Size = new System.Drawing.Size(357, 49);
            this.lbl_addUser.TabIndex = 8;
            this.lbl_addUser.Text = "AÑADIR EVENTO";
            // 
            // fecha_evento
            // 
            this.fecha_evento.AutoSize = true;
            this.fecha_evento.Font = new System.Drawing.Font("Century Gothic", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.fecha_evento.ForeColor = System.Drawing.Color.DimGray;
            this.fecha_evento.Location = new System.Drawing.Point(35, 109);
            this.fecha_evento.Name = "fecha_evento";
            this.fecha_evento.Size = new System.Drawing.Size(156, 21);
            this.fecha_evento.TabIndex = 17;
            this.fecha_evento.Text = "FECHA DE EVENTO";
            // 
            // lbl_asunto
            // 
            this.lbl_asunto.AutoSize = true;
            this.lbl_asunto.Font = new System.Drawing.Font("Century Gothic", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lbl_asunto.ForeColor = System.Drawing.Color.DimGray;
            this.lbl_asunto.Location = new System.Drawing.Point(35, 147);
            this.lbl_asunto.Name = "lbl_asunto";
            this.lbl_asunto.Size = new System.Drawing.Size(175, 21);
            this.lbl_asunto.TabIndex = 19;
            this.lbl_asunto.Text = "ASUNTO DEL EVENTO";
            // 
            // txt_asunto
            // 
            this.txt_asunto.BackColor = System.Drawing.Color.WhiteSmoke;
            this.txt_asunto.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.txt_asunto.Font = new System.Drawing.Font("Century Gothic", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.txt_asunto.ForeColor = System.Drawing.Color.DimGray;
            this.txt_asunto.Location = new System.Drawing.Point(39, 180);
            this.txt_asunto.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.txt_asunto.Multiline = true;
            this.txt_asunto.Name = "txt_asunto";
            this.txt_asunto.Size = new System.Drawing.Size(420, 142);
            this.txt_asunto.TabIndex = 43;
            // 
            // pick_fecha_evento
            // 
            this.pick_fecha_evento.CustomFormat = "dd-MM-yyyy";
            this.pick_fecha_evento.Font = new System.Drawing.Font("Century Gothic", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.pick_fecha_evento.Format = System.Windows.Forms.DateTimePickerFormat.Custom;
            this.pick_fecha_evento.Location = new System.Drawing.Point(223, 109);
            this.pick_fecha_evento.MinDate = new System.DateTime(2020, 6, 25, 0, 0, 0, 0);
            this.pick_fecha_evento.Name = "pick_fecha_evento";
            this.pick_fecha_evento.Size = new System.Drawing.Size(187, 27);
            this.pick_fecha_evento.TabIndex = 44;
            this.pick_fecha_evento.Value = new System.DateTime(2020, 6, 25, 0, 0, 0, 0);
            this.pick_fecha_evento.ValueChanged += new System.EventHandler(this.pick_fecha_evento_ValueChanged);
            // 
            // lbl_participantes
            // 
            this.lbl_participantes.AutoSize = true;
            this.lbl_participantes.Font = new System.Drawing.Font("Century Gothic", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lbl_participantes.ForeColor = System.Drawing.Color.DimGray;
            this.lbl_participantes.Location = new System.Drawing.Point(35, 341);
            this.lbl_participantes.Name = "lbl_participantes";
            this.lbl_participantes.Size = new System.Drawing.Size(201, 21);
            this.lbl_participantes.TabIndex = 45;
            this.lbl_participantes.Text = "AÑADIR PARTICIPANTES";
            // 
            // panel_users
            // 
            this.panel_users.Location = new System.Drawing.Point(39, 373);
            this.panel_users.Name = "panel_users";
            this.panel_users.Size = new System.Drawing.Size(420, 130);
            this.panel_users.TabIndex = 46;
            // 
            // comboBox1
            // 
            this.comboBox1.Font = new System.Drawing.Font("Century Gothic", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.comboBox1.FormattingEnabled = true;
            this.comboBox1.Location = new System.Drawing.Point(242, 338);
            this.comboBox1.Name = "comboBox1";
            this.comboBox1.Size = new System.Drawing.Size(181, 29);
            this.comboBox1.TabIndex = 47;
            // 
            // bt_adduser
            // 
            this.bt_adduser.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(27)))), ((int)(((byte)(131)))), ((int)(((byte)(185)))));
            this.bt_adduser.FlatAppearance.BorderSize = 0;
            this.bt_adduser.FlatAppearance.MouseDownBackColor = System.Drawing.Color.FromArgb(((int)(((byte)(28)))), ((int)(((byte)(28)))), ((int)(((byte)(28)))));
            this.bt_adduser.FlatAppearance.MouseOverBackColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.bt_adduser.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.bt_adduser.Font = new System.Drawing.Font("Microsoft Sans Serif", 11F);
            this.bt_adduser.ForeColor = System.Drawing.Color.White;
            this.bt_adduser.Image = ((System.Drawing.Image)(resources.GetObject("bt_adduser.Image")));
            this.bt_adduser.Location = new System.Drawing.Point(429, 338);
            this.bt_adduser.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.bt_adduser.Name = "bt_adduser";
            this.bt_adduser.Size = new System.Drawing.Size(30, 29);
            this.bt_adduser.TabIndex = 48;
            this.bt_adduser.UseVisualStyleBackColor = false;
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
            this.backHome.Location = new System.Drawing.Point(39, 533);
            this.backHome.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.backHome.Name = "backHome";
            this.backHome.Size = new System.Drawing.Size(101, 33);
            this.backHome.TabIndex = 50;
            this.backHome.Text = "VOLVER";
            this.backHome.UseVisualStyleBackColor = false;
            // 
            // bt_add_event
            // 
            this.bt_add_event.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(27)))), ((int)(((byte)(131)))), ((int)(((byte)(185)))));
            this.bt_add_event.FlatAppearance.BorderSize = 0;
            this.bt_add_event.FlatAppearance.MouseDownBackColor = System.Drawing.Color.FromArgb(((int)(((byte)(28)))), ((int)(((byte)(28)))), ((int)(((byte)(28)))));
            this.bt_add_event.FlatAppearance.MouseOverBackColor = System.Drawing.Color.FromArgb(((int)(((byte)(64)))), ((int)(((byte)(64)))), ((int)(((byte)(64)))));
            this.bt_add_event.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.bt_add_event.Font = new System.Drawing.Font("Microsoft Sans Serif", 11F);
            this.bt_add_event.ForeColor = System.Drawing.Color.LightGray;
            this.bt_add_event.Location = new System.Drawing.Point(227, 533);
            this.bt_add_event.Margin = new System.Windows.Forms.Padding(3, 2, 3, 2);
            this.bt_add_event.Name = "bt_add_event";
            this.bt_add_event.Size = new System.Drawing.Size(232, 33);
            this.bt_add_event.TabIndex = 49;
            this.bt_add_event.Text = "AÑADIR";
            this.bt_add_event.UseVisualStyleBackColor = false;
            // 
            // Form_add_event
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.Color.White;
            this.ClientSize = new System.Drawing.Size(494, 591);
            this.Controls.Add(this.backHome);
            this.Controls.Add(this.bt_add_event);
            this.Controls.Add(this.bt_adduser);
            this.Controls.Add(this.comboBox1);
            this.Controls.Add(this.panel_users);
            this.Controls.Add(this.lbl_participantes);
            this.Controls.Add(this.pick_fecha_evento);
            this.Controls.Add(this.txt_asunto);
            this.Controls.Add(this.lbl_asunto);
            this.Controls.Add(this.fecha_evento);
            this.Controls.Add(this.lbl_addUser);
            this.Name = "Form_add_event";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Form_add_event";
            this.Load += new System.EventHandler(this.Form_add_event_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label lbl_addUser;
        private System.Windows.Forms.Label fecha_evento;
        private System.Windows.Forms.Label lbl_asunto;
        private System.Windows.Forms.TextBox txt_asunto;
        private System.Windows.Forms.DateTimePicker pick_fecha_evento;
        private System.Windows.Forms.Label lbl_participantes;
        private System.Windows.Forms.Panel panel_users;
        private System.Windows.Forms.ComboBox comboBox1;
        private System.Windows.Forms.Button bt_adduser;
        private System.Windows.Forms.Button backHome;
        private System.Windows.Forms.Button bt_add_event;
    }
}
namespace proyectocarlitos
{
    partial class Form1
    {
        /// <summary>
        /// Variable del diseñador requerida.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Limpiar los recursos que se estén utilizando.
        /// </summary>
        /// <param name="disposing">true si los recursos administrados se deben desechar; false en caso contrario.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Código generado por el Diseñador de Windows Forms

        /// <summary>
        /// Método necesario para admitir el Diseñador. No se puede modificar
        /// el contenido del método con el editor de código.
        /// </summary>
        private void InitializeComponent()
        {
            this.menuStrip1 = new System.Windows.Forms.MenuStrip();
            this.archivoToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.verArchivosToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.reportesToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.cerrarSesionToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.alumnoToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.altaDeAlumnoToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.modificarAlumnoToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.representanteToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.altaRepresentanteToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.modificarRepresentanteToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.empresaToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.altaEmpresaToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.modificarEmpresaToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.notificacionesToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.enviarNotificacionesToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.menuStrip1.SuspendLayout();
            this.SuspendLayout();
            // 
            // menuStrip1
            // 
            this.menuStrip1.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.archivoToolStripMenuItem,
            this.alumnoToolStripMenuItem,
            this.representanteToolStripMenuItem,
            this.empresaToolStripMenuItem,
            this.notificacionesToolStripMenuItem});
            this.menuStrip1.Location = new System.Drawing.Point(0, 0);
            this.menuStrip1.Name = "menuStrip1";
            this.menuStrip1.Size = new System.Drawing.Size(683, 24);
            this.menuStrip1.TabIndex = 0;
            this.menuStrip1.Text = "menuStrip1";
            // 
            // archivoToolStripMenuItem
            // 
            this.archivoToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.verArchivosToolStripMenuItem,
            this.cerrarSesionToolStripMenuItem});
            this.archivoToolStripMenuItem.Font = new System.Drawing.Font("Arial", 9.75F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))));
            this.archivoToolStripMenuItem.Name = "archivoToolStripMenuItem";
            this.archivoToolStripMenuItem.Size = new System.Drawing.Size(69, 20);
            this.archivoToolStripMenuItem.Text = "Archivo";
            // 
            // verArchivosToolStripMenuItem
            // 
            this.verArchivosToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.reportesToolStripMenuItem});
            this.verArchivosToolStripMenuItem.Name = "verArchivosToolStripMenuItem";
            this.verArchivosToolStripMenuItem.Size = new System.Drawing.Size(160, 22);
            this.verArchivosToolStripMenuItem.Text = "Ver Archivos";
            // 
            // reportesToolStripMenuItem
            // 
            this.reportesToolStripMenuItem.Name = "reportesToolStripMenuItem";
            this.reportesToolStripMenuItem.Size = new System.Drawing.Size(152, 22);
            this.reportesToolStripMenuItem.Text = "Reportes";
            this.reportesToolStripMenuItem.Click += new System.EventHandler(this.reportesToolStripMenuItem_Click);
            // 
            // cerrarSesionToolStripMenuItem
            // 
            this.cerrarSesionToolStripMenuItem.Name = "cerrarSesionToolStripMenuItem";
            this.cerrarSesionToolStripMenuItem.Size = new System.Drawing.Size(160, 22);
            this.cerrarSesionToolStripMenuItem.Text = "Cerrar Sesion";
            this.cerrarSesionToolStripMenuItem.Click += new System.EventHandler(this.cerrarSesionToolStripMenuItem_Click);
            // 
            // alumnoToolStripMenuItem
            // 
            this.alumnoToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.altaDeAlumnoToolStripMenuItem,
            this.modificarAlumnoToolStripMenuItem});
            this.alumnoToolStripMenuItem.Font = new System.Drawing.Font("Arial", 9.75F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))));
            this.alumnoToolStripMenuItem.Name = "alumnoToolStripMenuItem";
            this.alumnoToolStripMenuItem.Size = new System.Drawing.Size(70, 20);
            this.alumnoToolStripMenuItem.Text = "Alumno";
            // 
            // altaDeAlumnoToolStripMenuItem
            // 
            this.altaDeAlumnoToolStripMenuItem.Name = "altaDeAlumnoToolStripMenuItem";
            this.altaDeAlumnoToolStripMenuItem.Size = new System.Drawing.Size(186, 22);
            this.altaDeAlumnoToolStripMenuItem.Text = "Alta de Alumno";
            this.altaDeAlumnoToolStripMenuItem.Click += new System.EventHandler(this.altaDeAlumnoToolStripMenuItem_Click);
            // 
            // modificarAlumnoToolStripMenuItem
            // 
            this.modificarAlumnoToolStripMenuItem.Name = "modificarAlumnoToolStripMenuItem";
            this.modificarAlumnoToolStripMenuItem.Size = new System.Drawing.Size(186, 22);
            this.modificarAlumnoToolStripMenuItem.Text = "Modificar alumno";
            this.modificarAlumnoToolStripMenuItem.Click += new System.EventHandler(this.modificarAlumnoToolStripMenuItem_Click);
            // 
            // representanteToolStripMenuItem
            // 
            this.representanteToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.altaRepresentanteToolStripMenuItem,
            this.modificarRepresentanteToolStripMenuItem});
            this.representanteToolStripMenuItem.Font = new System.Drawing.Font("Arial", 9.75F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))));
            this.representanteToolStripMenuItem.Name = "representanteToolStripMenuItem";
            this.representanteToolStripMenuItem.Size = new System.Drawing.Size(111, 20);
            this.representanteToolStripMenuItem.Text = "Representante ";
            // 
            // altaRepresentanteToolStripMenuItem
            // 
            this.altaRepresentanteToolStripMenuItem.Name = "altaRepresentanteToolStripMenuItem";
            this.altaRepresentanteToolStripMenuItem.Size = new System.Drawing.Size(226, 22);
            this.altaRepresentanteToolStripMenuItem.Text = "Alta Representante";
            this.altaRepresentanteToolStripMenuItem.Click += new System.EventHandler(this.altaRepresentanteToolStripMenuItem_Click);
            // 
            // modificarRepresentanteToolStripMenuItem
            // 
            this.modificarRepresentanteToolStripMenuItem.Name = "modificarRepresentanteToolStripMenuItem";
            this.modificarRepresentanteToolStripMenuItem.Size = new System.Drawing.Size(226, 22);
            this.modificarRepresentanteToolStripMenuItem.Text = "Modificar Representante";
            this.modificarRepresentanteToolStripMenuItem.Click += new System.EventHandler(this.modificarRepresentanteToolStripMenuItem_Click);
            // 
            // empresaToolStripMenuItem
            // 
            this.empresaToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.altaEmpresaToolStripMenuItem,
            this.modificarEmpresaToolStripMenuItem});
            this.empresaToolStripMenuItem.Font = new System.Drawing.Font("Arial", 9.75F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))));
            this.empresaToolStripMenuItem.Name = "empresaToolStripMenuItem";
            this.empresaToolStripMenuItem.Size = new System.Drawing.Size(78, 20);
            this.empresaToolStripMenuItem.Text = "Empresa ";
            // 
            // altaEmpresaToolStripMenuItem
            // 
            this.altaEmpresaToolStripMenuItem.Name = "altaEmpresaToolStripMenuItem";
            this.altaEmpresaToolStripMenuItem.Size = new System.Drawing.Size(193, 22);
            this.altaEmpresaToolStripMenuItem.Text = "Alta Empresa";
            this.altaEmpresaToolStripMenuItem.Click += new System.EventHandler(this.altaEmpresaToolStripMenuItem_Click);
            // 
            // modificarEmpresaToolStripMenuItem
            // 
            this.modificarEmpresaToolStripMenuItem.Name = "modificarEmpresaToolStripMenuItem";
            this.modificarEmpresaToolStripMenuItem.Size = new System.Drawing.Size(193, 22);
            this.modificarEmpresaToolStripMenuItem.Text = "Modificar Empresa";
            this.modificarEmpresaToolStripMenuItem.Click += new System.EventHandler(this.modificarEmpresaToolStripMenuItem_Click);
            // 
            // notificacionesToolStripMenuItem
            // 
            this.notificacionesToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.enviarNotificacionesToolStripMenuItem});
            this.notificacionesToolStripMenuItem.Font = new System.Drawing.Font("Arial", 9.75F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))));
            this.notificacionesToolStripMenuItem.Name = "notificacionesToolStripMenuItem";
            this.notificacionesToolStripMenuItem.Size = new System.Drawing.Size(107, 20);
            this.notificacionesToolStripMenuItem.Text = "Notificaciones";
            // 
            // enviarNotificacionesToolStripMenuItem
            // 
            this.enviarNotificacionesToolStripMenuItem.Name = "enviarNotificacionesToolStripMenuItem";
            this.enviarNotificacionesToolStripMenuItem.Size = new System.Drawing.Size(207, 22);
            this.enviarNotificacionesToolStripMenuItem.Text = "Enviar Notificaciones";
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(683, 296);
            this.Controls.Add(this.menuStrip1);
            this.MainMenuStrip = this.menuStrip1;
            this.MaximizeBox = false;
            this.Name = "Form1";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Sistema UAG";
            this.menuStrip1.ResumeLayout(false);
            this.menuStrip1.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.MenuStrip menuStrip1;
        private System.Windows.Forms.ToolStripMenuItem archivoToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem cerrarSesionToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem alumnoToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem altaDeAlumnoToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem modificarAlumnoToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem representanteToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem notificacionesToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem verArchivosToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem reportesToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem altaRepresentanteToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem empresaToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem altaEmpresaToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem enviarNotificacionesToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem modificarRepresentanteToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem modificarEmpresaToolStripMenuItem;
    }
}

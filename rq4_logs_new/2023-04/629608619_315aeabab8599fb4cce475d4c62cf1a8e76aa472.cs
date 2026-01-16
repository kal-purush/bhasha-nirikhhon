namespace CorrectivoOPreventivo.Controles
{
    partial class ControlMantenimientoCorrectivo
    {
        /// <summary> 
        /// Variable del diseñador necesaria.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary> 
        /// Limpiar los recursos que se estén usando.
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

        #region Código generado por el Diseñador de componentes

        /// <summary> 
        /// Método necesario para admitir el Diseñador. No se puede modificar
        /// el contenido de este método con el editor de código.
        /// </summary>
        private void InitializeComponent()
        {
            this.dgvCorrectivo = new System.Windows.Forms.DataGridView();
            this.dia = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.rnd_averia = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.dia_para_reparo = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.dia_reparar = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.se_reparo = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.Ultimo_dia_arreglado = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.costo_reparacion = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.acumulacion_costo = new System.Windows.Forms.DataGridViewTextBoxColumn();
            ((System.ComponentModel.ISupportInitialize)(this.dgvCorrectivo)).BeginInit();
            this.SuspendLayout();
            // 
            // dgvCorrectivo
            // 
            this.dgvCorrectivo.AllowUserToAddRows = false;
            this.dgvCorrectivo.AllowUserToDeleteRows = false;
            this.dgvCorrectivo.BackgroundColor = System.Drawing.SystemColors.ButtonHighlight;
            this.dgvCorrectivo.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dgvCorrectivo.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.dia,
            this.rnd_averia,
            this.dia_para_reparo,
            this.dia_reparar,
            this.se_reparo,
            this.Ultimo_dia_arreglado,
            this.costo_reparacion,
            this.acumulacion_costo});
            this.dgvCorrectivo.Dock = System.Windows.Forms.DockStyle.Fill;
            this.dgvCorrectivo.Location = new System.Drawing.Point(0, 0);
            this.dgvCorrectivo.MinimumSize = new System.Drawing.Size(1053, 341);
            this.dgvCorrectivo.Name = "dgvCorrectivo";
            this.dgvCorrectivo.RowHeadersWidth = 51;
            this.dgvCorrectivo.RowTemplate.Height = 29;
            this.dgvCorrectivo.Size = new System.Drawing.Size(1053, 341);
            this.dgvCorrectivo.TabIndex = 1;
            // 
            // dia
            // 
            this.dia.AutoSizeMode = System.Windows.Forms.DataGridViewAutoSizeColumnMode.Fill;
            this.dia.HeaderText = "Dia";
            this.dia.MinimumWidth = 6;
            this.dia.Name = "dia";
            this.dia.ReadOnly = true;
            // 
            // rnd_averia
            // 
            this.rnd_averia.AutoSizeMode = System.Windows.Forms.DataGridViewAutoSizeColumnMode.Fill;
            this.rnd_averia.HeaderText = "RND Averia";
            this.rnd_averia.MinimumWidth = 6;
            this.rnd_averia.Name = "rnd_averia";
            this.rnd_averia.ReadOnly = true;
            // 
            // dia_para_reparo
            // 
            this.dia_para_reparo.AutoSizeMode = System.Windows.Forms.DataGridViewAutoSizeColumnMode.Fill;
            this.dia_para_reparo.HeaderText = "Dias Para El Reparo";
            this.dia_para_reparo.MinimumWidth = 6;
            this.dia_para_reparo.Name = "dia_para_reparo";
            this.dia_para_reparo.ReadOnly = true;
            // 
            // dia_reparar
            // 
            this.dia_reparar.AutoSizeMode = System.Windows.Forms.DataGridViewAutoSizeColumnMode.Fill;
            this.dia_reparar.HeaderText = "Dia A Reparar";
            this.dia_reparar.MinimumWidth = 6;
            this.dia_reparar.Name = "dia_reparar";
            this.dia_reparar.ReadOnly = true;
            // 
            // se_reparo
            // 
            this.se_reparo.AutoSizeMode = System.Windows.Forms.DataGridViewAutoSizeColumnMode.Fill;
            this.se_reparo.HeaderText = "Se reparo";
            this.se_reparo.MinimumWidth = 6;
            this.se_reparo.Name = "se_reparo";
            this.se_reparo.ReadOnly = true;
            // 
            // Ultimo_dia_arreglado
            // 
            this.Ultimo_dia_arreglado.AutoSizeMode = System.Windows.Forms.DataGridViewAutoSizeColumnMode.Fill;
            this.Ultimo_dia_arreglado.HeaderText = "Ultimo Dia Arreglado";
            this.Ultimo_dia_arreglado.MinimumWidth = 6;
            this.Ultimo_dia_arreglado.Name = "Ultimo_dia_arreglado";
            this.Ultimo_dia_arreglado.ReadOnly = true;
            // 
            // costo_reparacion
            // 
            this.costo_reparacion.AutoSizeMode = System.Windows.Forms.DataGridViewAutoSizeColumnMode.Fill;
            this.costo_reparacion.HeaderText = "Costo de reparacion";
            this.costo_reparacion.MinimumWidth = 6;
            this.costo_reparacion.Name = "costo_reparacion";
            // 
            // acumulacion_costo
            // 
            this.acumulacion_costo.AutoSizeMode = System.Windows.Forms.DataGridViewAutoSizeColumnMode.Fill;
            this.acumulacion_costo.HeaderText = "Acumulacion Costo";
            this.acumulacion_costo.MinimumWidth = 6;
            this.acumulacion_costo.Name = "acumulacion_costo";
            this.acumulacion_costo.ReadOnly = true;
            // 
            // ControlMantenimientoCorrectivo
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(7F, 15F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.dgvCorrectivo);
            this.Name = "ControlMantenimientoCorrectivo";
            this.Size = new System.Drawing.Size(1053, 341);
            ((System.ComponentModel.ISupportInitialize)(this.dgvCorrectivo)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion

        private DataGridView dgvCorrectivo;
        private DataGridViewTextBoxColumn dia;
        private DataGridViewTextBoxColumn rnd_averia;
        private DataGridViewTextBoxColumn dia_para_reparo;
        private DataGridViewTextBoxColumn dia_reparar;
        private DataGridViewTextBoxColumn se_reparo;
        private DataGridViewTextBoxColumn Ultimo_dia_arreglado;
        private DataGridViewTextBoxColumn costo_reparacion;
        private DataGridViewTextBoxColumn acumulacion_costo;
    }
}
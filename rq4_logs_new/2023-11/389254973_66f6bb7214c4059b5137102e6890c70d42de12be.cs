
namespace papeSraErika
{
    partial class ProductView
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
            this.components = new System.ComponentModel.Container();
            System.Windows.Forms.DataGridViewCellStyle dataGridViewCellStyle1 = new System.Windows.Forms.DataGridViewCellStyle();
            this.productTable = new System.Windows.Forms.DataGridView();
            this.textSearch = new System.Windows.Forms.TextBox();
            this.BuscarLbl = new System.Windows.Forms.Label();
            this.Productos = new System.Windows.Forms.Label();
            this.AgregarStock = new System.Windows.Forms.Button();
            this.ElimunarBtn = new System.Windows.Forms.Button();
            this.ActualizarBtn = new System.Windows.Forms.Button();
            this.EditarProducto = new System.Windows.Forms.Button();
            this.AgregarBtn = new System.Windows.Forms.Button();
            this.CerrarBtn = new System.Windows.Forms.Button();
            this.productMapBindingSource = new System.Windows.Forms.BindingSource(this.components);
            this.barCodeDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.productNameDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.price1DataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.stock1DataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            ((System.ComponentModel.ISupportInitialize)(this.productTable)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.productMapBindingSource)).BeginInit();
            this.SuspendLayout();
            // 
            // productTable
            // 
            this.productTable.AllowUserToAddRows = false;
            this.productTable.AllowUserToDeleteRows = false;
            this.productTable.AutoGenerateColumns = false;
            this.productTable.BackgroundColor = System.Drawing.SystemColors.ActiveCaption;
            this.productTable.ColumnHeadersHeight = 29;
            this.productTable.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.DisableResizing;
            this.productTable.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.barCodeDataGridViewTextBoxColumn,
            this.productNameDataGridViewTextBoxColumn,
            this.price1DataGridViewTextBoxColumn,
            this.stock1DataGridViewTextBoxColumn});
            this.productTable.DataSource = this.productMapBindingSource;
            this.productTable.Location = new System.Drawing.Point(140, 169);
            this.productTable.Margin = new System.Windows.Forms.Padding(4);
            this.productTable.Name = "productTable";
            this.productTable.ReadOnly = true;
            dataGridViewCellStyle1.Alignment = System.Windows.Forms.DataGridViewContentAlignment.MiddleLeft;
            dataGridViewCellStyle1.BackColor = System.Drawing.SystemColors.Control;
            dataGridViewCellStyle1.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            dataGridViewCellStyle1.ForeColor = System.Drawing.SystemColors.WindowText;
            dataGridViewCellStyle1.SelectionBackColor = System.Drawing.SystemColors.Highlight;
            dataGridViewCellStyle1.SelectionForeColor = System.Drawing.SystemColors.ControlText;
            dataGridViewCellStyle1.WrapMode = System.Windows.Forms.DataGridViewTriState.True;
            this.productTable.RowHeadersDefaultCellStyle = dataGridViewCellStyle1;
            this.productTable.RowHeadersVisible = false;
            this.productTable.RowHeadersWidth = 51;
            this.productTable.RowTemplate.DefaultCellStyle.BackColor = System.Drawing.Color.White;
            this.productTable.RowTemplate.DefaultCellStyle.ForeColor = System.Drawing.Color.Black;
            this.productTable.Size = new System.Drawing.Size(1028, 479);
            this.productTable.TabIndex = 0;
            // 
            // textSearch
            // 
            this.textSearch.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.textSearch.Location = new System.Drawing.Point(487, 97);
            this.textSearch.Margin = new System.Windows.Forms.Padding(4);
            this.textSearch.Name = "textSearch";
            this.textSearch.Size = new System.Drawing.Size(461, 26);
            this.textSearch.TabIndex = 3;
            this.textSearch.TextChanged += new System.EventHandler(this.textSearch_TextChanged);
            // 
            // BuscarLbl
            // 
            this.BuscarLbl.AutoSize = true;
            this.BuscarLbl.Font = new System.Drawing.Font("Arial Narrow", 14.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.BuscarLbl.ForeColor = System.Drawing.Color.Transparent;
            this.BuscarLbl.Location = new System.Drawing.Point(381, 97);
            this.BuscarLbl.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.BuscarLbl.Name = "BuscarLbl";
            this.BuscarLbl.Size = new System.Drawing.Size(75, 29);
            this.BuscarLbl.TabIndex = 4;
            this.BuscarLbl.Text = "Buscar";
            // 
            // Productos
            // 
            this.Productos.AutoSize = true;
            this.Productos.Font = new System.Drawing.Font("Microsoft Sans Serif", 20.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Productos.ForeColor = System.Drawing.Color.Transparent;
            this.Productos.Location = new System.Drawing.Point(615, 11);
            this.Productos.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.Productos.Name = "Productos";
            this.Productos.Size = new System.Drawing.Size(170, 39);
            this.Productos.TabIndex = 8;
            this.Productos.Text = "Productos";
            // 
            // AgregarStock
            // 
            this.AgregarStock.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(0)))), ((int)(((byte)(192)))), ((int)(((byte)(0)))));
            this.AgregarStock.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.AgregarStock.FlatAppearance.BorderSize = 0;
            this.AgregarStock.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.AgregarStock.Location = new System.Drawing.Point(1217, 192);
            this.AgregarStock.Margin = new System.Windows.Forms.Padding(4);
            this.AgregarStock.Name = "AgregarStock";
            this.AgregarStock.Size = new System.Drawing.Size(167, 48);
            this.AgregarStock.TabIndex = 9;
            this.AgregarStock.Text = "Agregar stock";
            this.AgregarStock.UseVisualStyleBackColor = false;
            this.AgregarStock.Click += new System.EventHandler(this.addStock_Click);
            // 
            // ElimunarBtn
            // 
            this.ElimunarBtn.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(192)))), ((int)(((byte)(0)))), ((int)(((byte)(0)))));
            this.ElimunarBtn.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.ElimunarBtn.FlatAppearance.BorderSize = 0;
            this.ElimunarBtn.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.ElimunarBtn.ForeColor = System.Drawing.Color.White;
            this.ElimunarBtn.Location = new System.Drawing.Point(1217, 394);
            this.ElimunarBtn.Margin = new System.Windows.Forms.Padding(4);
            this.ElimunarBtn.Name = "ElimunarBtn";
            this.ElimunarBtn.Size = new System.Drawing.Size(167, 53);
            this.ElimunarBtn.TabIndex = 6;
            this.ElimunarBtn.Text = "Eliminar";
            this.ElimunarBtn.UseVisualStyleBackColor = false;
            this.ElimunarBtn.Click += new System.EventHandler(this.btnDelete_Click);
            // 
            // ActualizarBtn
            // 
            this.ActualizarBtn.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(192)))), ((int)(((byte)(0)))), ((int)(((byte)(0)))));
            this.ActualizarBtn.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.ActualizarBtn.FlatAppearance.BorderSize = 0;
            this.ActualizarBtn.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.ActualizarBtn.Location = new System.Drawing.Point(1217, 501);
            this.ActualizarBtn.Margin = new System.Windows.Forms.Padding(4);
            this.ActualizarBtn.Name = "ActualizarBtn";
            this.ActualizarBtn.Size = new System.Drawing.Size(167, 64);
            this.ActualizarBtn.TabIndex = 5;
            this.ActualizarBtn.Text = "Actualizar";
            this.ActualizarBtn.UseVisualStyleBackColor = false;
            this.ActualizarBtn.Click += new System.EventHandler(this.button3_Click);
            // 
            // EditarProducto
            // 
            this.EditarProducto.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(255)))), ((int)(((byte)(128)))), ((int)(((byte)(0)))));
            this.EditarProducto.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.EditarProducto.FlatAppearance.BorderSize = 0;
            this.EditarProducto.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.EditarProducto.Location = new System.Drawing.Point(1217, 282);
            this.EditarProducto.Margin = new System.Windows.Forms.Padding(4);
            this.EditarProducto.Name = "EditarProducto";
            this.EditarProducto.Size = new System.Drawing.Size(167, 60);
            this.EditarProducto.TabIndex = 2;
            this.EditarProducto.Text = "Editar";
            this.EditarProducto.UseVisualStyleBackColor = false;
            this.EditarProducto.Click += new System.EventHandler(this.editProduct_Click);
            // 
            // AgregarBtn
            // 
            this.AgregarBtn.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(0)))), ((int)(((byte)(192)))), ((int)(((byte)(0)))));
            this.AgregarBtn.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.AgregarBtn.FlatAppearance.BorderSize = 0;
            this.AgregarBtn.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.AgregarBtn.Location = new System.Drawing.Point(1217, 97);
            this.AgregarBtn.Margin = new System.Windows.Forms.Padding(4);
            this.AgregarBtn.Name = "AgregarBtn";
            this.AgregarBtn.Size = new System.Drawing.Size(167, 48);
            this.AgregarBtn.TabIndex = 1;
            this.AgregarBtn.Text = "Agregar Producto";
            this.AgregarBtn.UseVisualStyleBackColor = false;
            this.AgregarBtn.Click += new System.EventHandler(this.button1_Click);
            // 
            // CerrarBtn
            // 
            this.CerrarBtn.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(192)))), ((int)(((byte)(0)))), ((int)(((byte)(0)))));
            this.CerrarBtn.BackgroundImageLayout = System.Windows.Forms.ImageLayout.Stretch;
            this.CerrarBtn.FlatAppearance.BorderSize = 0;
            this.CerrarBtn.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.CerrarBtn.Location = new System.Drawing.Point(1217, 612);
            this.CerrarBtn.Margin = new System.Windows.Forms.Padding(4);
            this.CerrarBtn.Name = "CerrarBtn";
            this.CerrarBtn.Size = new System.Drawing.Size(167, 64);
            this.CerrarBtn.TabIndex = 10;
            this.CerrarBtn.Text = "Cerrar";
            this.CerrarBtn.UseVisualStyleBackColor = false;
            this.CerrarBtn.Click += new System.EventHandler(this.button2_Click);
            // 
            // productMapBindingSource
            // 
            this.productMapBindingSource.DataSource = typeof(papeSraErika.ProductMap);
            // 
            // barCodeDataGridViewTextBoxColumn
            // 
            this.barCodeDataGridViewTextBoxColumn.DataPropertyName = "BarCode";
            this.barCodeDataGridViewTextBoxColumn.HeaderText = "Codigo de barras";
            this.barCodeDataGridViewTextBoxColumn.MinimumWidth = 6;
            this.barCodeDataGridViewTextBoxColumn.Name = "barCodeDataGridViewTextBoxColumn";
            this.barCodeDataGridViewTextBoxColumn.ReadOnly = true;
            this.barCodeDataGridViewTextBoxColumn.Width = 150;
            // 
            // productNameDataGridViewTextBoxColumn
            // 
            this.productNameDataGridViewTextBoxColumn.DataPropertyName = "ProductName";
            this.productNameDataGridViewTextBoxColumn.HeaderText = "Producto";
            this.productNameDataGridViewTextBoxColumn.MinimumWidth = 6;
            this.productNameDataGridViewTextBoxColumn.Name = "productNameDataGridViewTextBoxColumn";
            this.productNameDataGridViewTextBoxColumn.ReadOnly = true;
            this.productNameDataGridViewTextBoxColumn.Width = 200;
            // 
            // price1DataGridViewTextBoxColumn
            // 
            this.price1DataGridViewTextBoxColumn.DataPropertyName = "Price1";
            this.price1DataGridViewTextBoxColumn.HeaderText = "Precio";
            this.price1DataGridViewTextBoxColumn.MinimumWidth = 6;
            this.price1DataGridViewTextBoxColumn.Name = "price1DataGridViewTextBoxColumn";
            this.price1DataGridViewTextBoxColumn.ReadOnly = true;
            this.price1DataGridViewTextBoxColumn.Width = 200;
            // 
            // stock1DataGridViewTextBoxColumn
            // 
            this.stock1DataGridViewTextBoxColumn.DataPropertyName = "Stock1";
            this.stock1DataGridViewTextBoxColumn.HeaderText = "Stock";
            this.stock1DataGridViewTextBoxColumn.MinimumWidth = 6;
            this.stock1DataGridViewTextBoxColumn.Name = "stock1DataGridViewTextBoxColumn";
            this.stock1DataGridViewTextBoxColumn.ReadOnly = true;
            this.stock1DataGridViewTextBoxColumn.Width = 200;
            // 
            // ProductView
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.SystemColors.Highlight;
            this.ClientSize = new System.Drawing.Size(1417, 690);
            this.Controls.Add(this.CerrarBtn);
            this.Controls.Add(this.AgregarStock);
            this.Controls.Add(this.Productos);
            this.Controls.Add(this.ElimunarBtn);
            this.Controls.Add(this.ActualizarBtn);
            this.Controls.Add(this.BuscarLbl);
            this.Controls.Add(this.textSearch);
            this.Controls.Add(this.EditarProducto);
            this.Controls.Add(this.AgregarBtn);
            this.Controls.Add(this.productTable);
            this.ForeColor = System.Drawing.Color.Transparent;
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.None;
            this.Margin = new System.Windows.Forms.Padding(4);
            this.Name = "ProductView";
            this.Text = "productView";
            ((System.ComponentModel.ISupportInitialize)(this.productTable)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.productMapBindingSource)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.DataGridView productTable;
        private System.Windows.Forms.Button AgregarBtn;
        private System.Windows.Forms.Button EditarProducto;
        private System.Windows.Forms.TextBox textSearch;
        private System.Windows.Forms.Label BuscarLbl;
        private System.Windows.Forms.Button ActualizarBtn;
        private System.Windows.Forms.Button ElimunarBtn;
        private System.Windows.Forms.Label Productos;
        private System.Windows.Forms.Button AgregarStock;
        private System.Windows.Forms.Button CerrarBtn;
        private System.Windows.Forms.BindingSource productMapBindingSource;
        private System.Windows.Forms.DataGridViewTextBoxColumn barCodeDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn productNameDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn price1DataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn stock1DataGridViewTextBoxColumn;
    }
}
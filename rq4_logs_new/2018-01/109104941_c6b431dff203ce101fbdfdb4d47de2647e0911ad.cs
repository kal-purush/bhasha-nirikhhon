namespace CarDealership.Controls
{
    partial class CarsView
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

        #region Component Designer generated code

        /// <summary> 
        /// Required method for Designer support - do not modify 
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.dataGridView1 = new System.Windows.Forms.DataGridView();
            this.Car_Vin = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.CAR_MODEL = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.CAR_PRICE = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.CAR_PRODUCTION = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.CAR_COLOR = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.CAR_DEALERSHIP = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.Accessories = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.Ordered = new System.Windows.Forms.DataGridViewCheckBoxColumn();
            ((System.ComponentModel.ISupportInitialize)(this.dataGridView1)).BeginInit();
            this.SuspendLayout();
            // 
            // dataGridView1
            // 
            this.dataGridView1.AllowUserToAddRows = false;
            this.dataGridView1.AllowUserToDeleteRows = false;
            this.dataGridView1.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dataGridView1.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.Car_Vin,
            this.CAR_MODEL,
            this.CAR_PRICE,
            this.CAR_PRODUCTION,
            this.CAR_COLOR,
            this.CAR_DEALERSHIP,
            this.Accessories,
            this.Ordered});
            this.dataGridView1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.dataGridView1.Location = new System.Drawing.Point(0, 0);
            this.dataGridView1.MultiSelect = false;
            this.dataGridView1.Name = "dataGridView1";
            this.dataGridView1.ReadOnly = true;
            this.dataGridView1.SelectionMode = System.Windows.Forms.DataGridViewSelectionMode.FullRowSelect;
            this.dataGridView1.Size = new System.Drawing.Size(844, 239);
            this.dataGridView1.TabIndex = 0;
            // 
            // Car_Vin
            // 
            this.Car_Vin.HeaderText = "VIN Number";
            this.Car_Vin.Name = "Car_Vin";
            this.Car_Vin.ReadOnly = true;
            // 
            // CAR_MODEL
            // 
            this.CAR_MODEL.HeaderText = "Model";
            this.CAR_MODEL.Name = "CAR_MODEL";
            this.CAR_MODEL.ReadOnly = true;
            // 
            // CAR_PRICE
            // 
            this.CAR_PRICE.HeaderText = "Base Price";
            this.CAR_PRICE.Name = "CAR_PRICE";
            this.CAR_PRICE.ReadOnly = true;
            // 
            // CAR_PRODUCTION
            // 
            this.CAR_PRODUCTION.HeaderText = "Production Year";
            this.CAR_PRODUCTION.Name = "CAR_PRODUCTION";
            this.CAR_PRODUCTION.ReadOnly = true;
            // 
            // CAR_COLOR
            // 
            this.CAR_COLOR.HeaderText = "Colour";
            this.CAR_COLOR.Name = "CAR_COLOR";
            this.CAR_COLOR.ReadOnly = true;
            // 
            // CAR_DEALERSHIP
            // 
            this.CAR_DEALERSHIP.HeaderText = "Dealership";
            this.CAR_DEALERSHIP.Name = "CAR_DEALERSHIP";
            this.CAR_DEALERSHIP.ReadOnly = true;
            // 
            // Accessories
            // 
            this.Accessories.HeaderText = "Accessories";
            this.Accessories.Name = "Accessories";
            this.Accessories.ReadOnly = true;
            // 
            // Ordered
            // 
            this.Ordered.HeaderText = "Ordered";
            this.Ordered.Name = "Ordered";
            this.Ordered.ReadOnly = true;
            // 
            // CarsView
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.dataGridView1);
            this.Name = "CarsView";
            this.Size = new System.Drawing.Size(844, 239);
            ((System.ComponentModel.ISupportInitialize)(this.dataGridView1)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.DataGridView dataGridView1;
        private System.Windows.Forms.DataGridViewTextBoxColumn Car_Vin;
        private System.Windows.Forms.DataGridViewTextBoxColumn CAR_MODEL;
        private System.Windows.Forms.DataGridViewTextBoxColumn CAR_PRICE;
        private System.Windows.Forms.DataGridViewTextBoxColumn CAR_PRODUCTION;
        private System.Windows.Forms.DataGridViewTextBoxColumn CAR_COLOR;
        private System.Windows.Forms.DataGridViewTextBoxColumn CAR_DEALERSHIP;
        private System.Windows.Forms.DataGridViewTextBoxColumn Accessories;
        private System.Windows.Forms.DataGridViewCheckBoxColumn Ordered;
    }
}
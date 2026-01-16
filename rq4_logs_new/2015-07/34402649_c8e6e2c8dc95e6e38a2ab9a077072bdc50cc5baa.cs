namespace SupplyChainManagementUI
{
    partial class OutputTableForm
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
            this.saveButton = new System.Windows.Forms.Button();
            this.itemsDataGrid = new System.Windows.Forms.DataGridView();
            this.overTimeDataGrid = new System.Windows.Forms.DataGridView();
            ((System.ComponentModel.ISupportInitialize)(this.itemsDataGrid)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.overTimeDataGrid)).BeginInit();
            this.SuspendLayout();
            // 
            // saveButton
            // 
            this.saveButton.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Right)));
            this.saveButton.Location = new System.Drawing.Point(912, 449);
            this.saveButton.Name = "saveButton";
            this.saveButton.Size = new System.Drawing.Size(113, 23);
            this.saveButton.TabIndex = 1;
            this.saveButton.Text = "Export as XML file";
            this.saveButton.UseVisualStyleBackColor = true;
            this.saveButton.Click += new System.EventHandler(this.saveButton_Click);
            // 
            // itemsDataGrid
            // 
            this.itemsDataGrid.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.itemsDataGrid.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.itemsDataGrid.Location = new System.Drawing.Point(12, 12);
            this.itemsDataGrid.Name = "itemsDataGrid";
            this.itemsDataGrid.Size = new System.Drawing.Size(626, 431);
            this.itemsDataGrid.TabIndex = 2;
            this.itemsDataGrid.CellContentClick += new System.Windows.Forms.DataGridViewCellEventHandler(this.dataGridView1_CellContentClick);
            // 
            // overTimeDataGrid
            // 
            this.overTimeDataGrid.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.overTimeDataGrid.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.overTimeDataGrid.Location = new System.Drawing.Point(644, 12);
            this.overTimeDataGrid.Name = "overTimeDataGrid";
            this.overTimeDataGrid.Size = new System.Drawing.Size(381, 431);
            this.overTimeDataGrid.TabIndex = 3;
            this.overTimeDataGrid.CellContentClick += new System.Windows.Forms.DataGridViewCellEventHandler(this.overTimeDataGrid_CellContentClick);
            // 
            // OutputTableForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1037, 484);
            this.Controls.Add(this.overTimeDataGrid);
            this.Controls.Add(this.itemsDataGrid);
            this.Controls.Add(this.saveButton);
            this.Name = "OutputTableForm";
            this.Text = "OutputXmlForm";
            this.Load += new System.EventHandler(this.OutputTableForm_Load);
            ((System.ComponentModel.ISupportInitialize)(this.itemsDataGrid)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.overTimeDataGrid)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Button saveButton;
        private System.Windows.Forms.DataGridView itemsDataGrid;
        private System.Windows.Forms.DataGridView overTimeDataGrid;
    }
}
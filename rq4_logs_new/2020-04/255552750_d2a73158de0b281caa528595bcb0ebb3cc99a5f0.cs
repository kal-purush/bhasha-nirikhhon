namespace QuanLyKhachSan.GUI
{
    partial class LichSuKhach
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
            this.dataGridViewLichSuCuaKhach = new System.Windows.Forms.DataGridView();
            this.ngayDenDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.ngayDiDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.maPDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.pROCLichSuThueBindingSource = new System.Windows.Forms.BindingSource(this.components);
            this.lichSuThueDataSet = new QuanLyKhachSan.LichSuThueDataSet();
            this.pROC_LichSuThueTableAdapter = new QuanLyKhachSan.LichSuThueDataSetTableAdapters.PROC_LichSuThueTableAdapter();
            this.label1 = new System.Windows.Forms.Label();
            ((System.ComponentModel.ISupportInitialize)(this.dataGridViewLichSuCuaKhach)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.pROCLichSuThueBindingSource)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.lichSuThueDataSet)).BeginInit();
            this.SuspendLayout();
            // 
            // dataGridViewLichSuCuaKhach
            // 
            this.dataGridViewLichSuCuaKhach.AutoGenerateColumns = false;
            this.dataGridViewLichSuCuaKhach.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dataGridViewLichSuCuaKhach.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.ngayDenDataGridViewTextBoxColumn,
            this.ngayDiDataGridViewTextBoxColumn,
            this.maPDataGridViewTextBoxColumn});
            this.dataGridViewLichSuCuaKhach.DataSource = this.pROCLichSuThueBindingSource;
            this.dataGridViewLichSuCuaKhach.Location = new System.Drawing.Point(1, 39);
            this.dataGridViewLichSuCuaKhach.Name = "dataGridViewLichSuCuaKhach";
            this.dataGridViewLichSuCuaKhach.Size = new System.Drawing.Size(444, 286);
            this.dataGridViewLichSuCuaKhach.TabIndex = 0;
            // 
            // ngayDenDataGridViewTextBoxColumn
            // 
            this.ngayDenDataGridViewTextBoxColumn.DataPropertyName = "NgayDen";
            this.ngayDenDataGridViewTextBoxColumn.HeaderText = "Ngày Đến";
            this.ngayDenDataGridViewTextBoxColumn.Name = "ngayDenDataGridViewTextBoxColumn";
            this.ngayDenDataGridViewTextBoxColumn.Width = 150;
            // 
            // ngayDiDataGridViewTextBoxColumn
            // 
            this.ngayDiDataGridViewTextBoxColumn.DataPropertyName = "NgayDi";
            this.ngayDiDataGridViewTextBoxColumn.HeaderText = "Ngày Đi";
            this.ngayDiDataGridViewTextBoxColumn.Name = "ngayDiDataGridViewTextBoxColumn";
            this.ngayDiDataGridViewTextBoxColumn.Width = 150;
            // 
            // maPDataGridViewTextBoxColumn
            // 
            this.maPDataGridViewTextBoxColumn.DataPropertyName = "MaP";
            this.maPDataGridViewTextBoxColumn.HeaderText = "Mã Phòng";
            this.maPDataGridViewTextBoxColumn.Name = "maPDataGridViewTextBoxColumn";
            // 
            // pROCLichSuThueBindingSource
            // 
            this.pROCLichSuThueBindingSource.DataMember = "PROC_LichSuThue";
            this.pROCLichSuThueBindingSource.DataSource = this.lichSuThueDataSet;
            // 
            // lichSuThueDataSet
            // 
            this.lichSuThueDataSet.DataSetName = "LichSuThueDataSet";
            this.lichSuThueDataSet.SchemaSerializationMode = System.Data.SchemaSerializationMode.IncludeSchema;
            // 
            // pROC_LichSuThueTableAdapter
            // 
            this.pROC_LichSuThueTableAdapter.ClearBeforeFill = true;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Times New Roman", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.Location = new System.Drawing.Point(113, 9);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(222, 19);
            this.label1.TabIndex = 1;
            this.label1.Text = "Lịch Sử Thuê Phòng Của Khách";
            // 
            // LichSuKhach
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(445, 324);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.dataGridViewLichSuCuaKhach);
            this.Name = "LichSuKhach";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "LichSuKhach";
            this.Load += new System.EventHandler(this.LichSuKhach_Load);
            ((System.ComponentModel.ISupportInitialize)(this.dataGridViewLichSuCuaKhach)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.pROCLichSuThueBindingSource)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.lichSuThueDataSet)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.DataGridView dataGridViewLichSuCuaKhach;
        private System.Windows.Forms.DataGridViewTextBoxColumn ngayDenDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn ngayDiDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn maPDataGridViewTextBoxColumn;
        private System.Windows.Forms.BindingSource pROCLichSuThueBindingSource;
        private LichSuThueDataSet lichSuThueDataSet;
        private LichSuThueDataSetTableAdapters.PROC_LichSuThueTableAdapter pROC_LichSuThueTableAdapter;
        private System.Windows.Forms.Label label1;
    }
}
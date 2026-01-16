namespace QL_TaXi
{
    partial class FormCacChuyenDiCuaTX
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
            this.label1 = new System.Windows.Forms.Label();
            this.dgvChuyenDiTX = new System.Windows.Forms.DataGridView();
            ((System.ComponentModel.ISupportInitialize)(this.dgvChuyenDiTX)).BeginInit();
            this.SuspendLayout();
            // 
            // label1
            // 
            this.label1.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(192)))), ((int)(((byte)(255)))), ((int)(((byte)(192)))));
            this.label1.BorderStyle = System.Windows.Forms.BorderStyle.Fixed3D;
            this.label1.Font = new System.Drawing.Font("Times New Roman", 18F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.Location = new System.Drawing.Point(1, 0);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(479, 51);
            this.label1.TabIndex = 0;
            this.label1.Text = "CÁC CHUYẾN XE CỦA TÀI XẾ";
            this.label1.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // dgvChuyenDiTX
            // 
            this.dgvChuyenDiTX.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dgvChuyenDiTX.Location = new System.Drawing.Point(1, 54);
            this.dgvChuyenDiTX.Name = "dgvChuyenDiTX";
            this.dgvChuyenDiTX.Size = new System.Drawing.Size(479, 202);
            this.dgvChuyenDiTX.TabIndex = 1;
            // 
            // FormCacChuyenDiCuaTX
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(480, 256);
            this.Controls.Add(this.dgvChuyenDiTX);
            this.Controls.Add(this.label1);
            this.Name = "FormCacChuyenDiCuaTX";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Danh Sách Chuyến Xe";
            this.Load += new System.EventHandler(this.FormCacChuyenDiCuaTX_Load);
            ((System.ComponentModel.ISupportInitialize)(this.dgvChuyenDiTX)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.DataGridView dgvChuyenDiTX;
    }
}
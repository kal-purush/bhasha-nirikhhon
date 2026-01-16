namespace FMSAPP
{
    partial class Seasomform
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
            this.dataGridView1 = new System.Windows.Forms.DataGridView();
            this.seasonIDDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.nameDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.createdatDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.deletedatDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.seasonBindingSource = new System.Windows.Forms.BindingSource(this.components);
            this.Created_Date_dpinker = new System.Windows.Forms.DateTimePicker();
            this.Creeated_day_lb = new System.Windows.Forms.Label();
            this.Delete_date_dpinker = new System.Windows.Forms.DateTimePicker();
            this.Delete_Date_lb = new System.Windows.Forms.Label();
            this.name = new System.Windows.Forms.Label();
            this.textBox1 = new System.Windows.Forms.TextBox();
            this.Insert_bt = new System.Windows.Forms.Button();
            this.Delete_bt = new System.Windows.Forms.Button();
            this.Update_bt = new System.Windows.Forms.Button();
            ((System.ComponentModel.ISupportInitialize)(this.dataGridView1)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.seasonBindingSource)).BeginInit();
            this.SuspendLayout();
            // 
            // dataGridView1
            // 
            this.dataGridView1.AutoGenerateColumns = false;
            this.dataGridView1.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dataGridView1.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.seasonIDDataGridViewTextBoxColumn,
            this.nameDataGridViewTextBoxColumn,
            this.createdatDataGridViewTextBoxColumn,
            this.deletedatDataGridViewTextBoxColumn});
            this.dataGridView1.DataSource = this.seasonBindingSource;
            this.dataGridView1.Location = new System.Drawing.Point(494, 2);
            this.dataGridView1.Name = "dataGridView1";
            this.dataGridView1.RowHeadersWidth = 51;
            this.dataGridView1.RowTemplate.Height = 24;
            this.dataGridView1.Size = new System.Drawing.Size(546, 449);
            this.dataGridView1.TabIndex = 0;
            // 
            // seasonIDDataGridViewTextBoxColumn
            // 
            this.seasonIDDataGridViewTextBoxColumn.DataPropertyName = "SeasonID";
            this.seasonIDDataGridViewTextBoxColumn.HeaderText = "SeasonID";
            this.seasonIDDataGridViewTextBoxColumn.MinimumWidth = 6;
            this.seasonIDDataGridViewTextBoxColumn.Name = "seasonIDDataGridViewTextBoxColumn";
            this.seasonIDDataGridViewTextBoxColumn.Width = 125;
            // 
            // nameDataGridViewTextBoxColumn
            // 
            this.nameDataGridViewTextBoxColumn.DataPropertyName = "name";
            this.nameDataGridViewTextBoxColumn.HeaderText = "name";
            this.nameDataGridViewTextBoxColumn.MinimumWidth = 6;
            this.nameDataGridViewTextBoxColumn.Name = "nameDataGridViewTextBoxColumn";
            this.nameDataGridViewTextBoxColumn.Width = 125;
            // 
            // createdatDataGridViewTextBoxColumn
            // 
            this.createdatDataGridViewTextBoxColumn.DataPropertyName = "created_at";
            this.createdatDataGridViewTextBoxColumn.HeaderText = "created_at";
            this.createdatDataGridViewTextBoxColumn.MinimumWidth = 6;
            this.createdatDataGridViewTextBoxColumn.Name = "createdatDataGridViewTextBoxColumn";
            this.createdatDataGridViewTextBoxColumn.Width = 125;
            // 
            // deletedatDataGridViewTextBoxColumn
            // 
            this.deletedatDataGridViewTextBoxColumn.DataPropertyName = "deleted_at";
            this.deletedatDataGridViewTextBoxColumn.HeaderText = "deleted_at";
            this.deletedatDataGridViewTextBoxColumn.MinimumWidth = 6;
            this.deletedatDataGridViewTextBoxColumn.Name = "deletedatDataGridViewTextBoxColumn";
            this.deletedatDataGridViewTextBoxColumn.Width = 125;
            // 
            // seasonBindingSource
            // 
            this.seasonBindingSource.DataSource = typeof(FMSAPP.season);
            // 
            // Created_Date_dpinker
            // 
            this.Created_Date_dpinker.Location = new System.Drawing.Point(173, 160);
            this.Created_Date_dpinker.Name = "Created_Date_dpinker";
            this.Created_Date_dpinker.Size = new System.Drawing.Size(234, 22);
            this.Created_Date_dpinker.TabIndex = 31;
            // 
            // Creeated_day_lb
            // 
            this.Creeated_day_lb.AutoSize = true;
            this.Creeated_day_lb.ImeMode = System.Windows.Forms.ImeMode.NoControl;
            this.Creeated_day_lb.Location = new System.Drawing.Point(42, 165);
            this.Creeated_day_lb.Name = "Creeated_day_lb";
            this.Creeated_day_lb.Size = new System.Drawing.Size(100, 17);
            this.Creeated_day_lb.TabIndex = 32;
            this.Creeated_day_lb.Text = "Created Date :";
            // 
            // Delete_date_dpinker
            // 
            this.Delete_date_dpinker.Location = new System.Drawing.Point(173, 240);
            this.Delete_date_dpinker.Name = "Delete_date_dpinker";
            this.Delete_date_dpinker.Size = new System.Drawing.Size(234, 22);
            this.Delete_date_dpinker.TabIndex = 33;
            // 
            // Delete_Date_lb
            // 
            this.Delete_Date_lb.AutoSize = true;
            this.Delete_Date_lb.ImeMode = System.Windows.Forms.ImeMode.NoControl;
            this.Delete_Date_lb.Location = new System.Drawing.Point(42, 245);
            this.Delete_Date_lb.Name = "Delete_Date_lb";
            this.Delete_Date_lb.Size = new System.Drawing.Size(91, 17);
            this.Delete_Date_lb.TabIndex = 34;
            this.Delete_Date_lb.Text = "Delete Date :";
            // 
            // name
            // 
            this.name.AutoSize = true;
            this.name.Location = new System.Drawing.Point(42, 79);
            this.name.Name = "name";
            this.name.Size = new System.Drawing.Size(53, 17);
            this.name.TabIndex = 35;
            this.name.Text = "Name :";
            // 
            // textBox1
            // 
            this.textBox1.Location = new System.Drawing.Point(173, 79);
            this.textBox1.Name = "textBox1";
            this.textBox1.Size = new System.Drawing.Size(234, 22);
            this.textBox1.TabIndex = 36;
            // 
            // Insert_bt
            // 
            this.Insert_bt.Location = new System.Drawing.Point(45, 333);
            this.Insert_bt.Name = "Insert_bt";
            this.Insert_bt.Size = new System.Drawing.Size(88, 23);
            this.Insert_bt.TabIndex = 37;
            this.Insert_bt.Text = "Insert";
            this.Insert_bt.UseVisualStyleBackColor = true;
            // 
            // Delete_bt
            // 
            this.Delete_bt.Location = new System.Drawing.Point(319, 333);
            this.Delete_bt.Name = "Delete_bt";
            this.Delete_bt.Size = new System.Drawing.Size(88, 23);
            this.Delete_bt.TabIndex = 38;
            this.Delete_bt.Text = "Delete";
            this.Delete_bt.UseVisualStyleBackColor = true;
            // 
            // Update_bt
            // 
            this.Update_bt.Location = new System.Drawing.Point(173, 333);
            this.Update_bt.Name = "Update_bt";
            this.Update_bt.Size = new System.Drawing.Size(88, 23);
            this.Update_bt.TabIndex = 39;
            this.Update_bt.Text = "Update";
            this.Update_bt.UseVisualStyleBackColor = true;
            // 
            // Seasomform
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1047, 518);
            this.Controls.Add(this.Update_bt);
            this.Controls.Add(this.Delete_bt);
            this.Controls.Add(this.Insert_bt);
            this.Controls.Add(this.textBox1);
            this.Controls.Add(this.name);
            this.Controls.Add(this.Delete_date_dpinker);
            this.Controls.Add(this.Delete_Date_lb);
            this.Controls.Add(this.Created_Date_dpinker);
            this.Controls.Add(this.Creeated_day_lb);
            this.Controls.Add(this.dataGridView1);
            this.Name = "Seasomform";
            this.Text = "Form1";
            this.Load += new System.EventHandler(this.Seasomform_Load);
            ((System.ComponentModel.ISupportInitialize)(this.dataGridView1)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.seasonBindingSource)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.DataGridView dataGridView1;
        private System.Windows.Forms.DataGridViewTextBoxColumn seasonIDDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn nameDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn createdatDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn deletedatDataGridViewTextBoxColumn;
        private System.Windows.Forms.BindingSource seasonBindingSource;
        private System.Windows.Forms.DateTimePicker Created_Date_dpinker;
        private System.Windows.Forms.Label Creeated_day_lb;
        private System.Windows.Forms.DateTimePicker Delete_date_dpinker;
        private System.Windows.Forms.Label Delete_Date_lb;
        private System.Windows.Forms.Label name;
        private System.Windows.Forms.TextBox textBox1;
        private System.Windows.Forms.Button Insert_bt;
        private System.Windows.Forms.Button Delete_bt;
        private System.Windows.Forms.Button Update_bt;
    }
}
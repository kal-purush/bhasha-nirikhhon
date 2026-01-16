namespace vp_proektna_biblioteka
{
    partial class AddBook
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
            this.booksDataSet = new vp_proektna_biblioteka.BooksDataSet();
            this.booksBindingSource = new System.Windows.Forms.BindingSource(this.components);
            this.booksTableAdapter = new vp_proektna_biblioteka.BooksDataSetTableAdapters.BooksTableAdapter();
            this.tableAdapterManager = new vp_proektna_biblioteka.BooksDataSetTableAdapters.TableAdapterManager();
            this.btnDodadiZacuvaj = new System.Windows.Forms.Button();
            this.label1 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.tbKategorija = new System.Windows.Forms.TextBox();
            this.tbAvtor = new System.Windows.Forms.TextBox();
            this.tbNaslov = new System.Windows.Forms.TextBox();
            ((System.ComponentModel.ISupportInitialize)(this.booksDataSet)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.booksBindingSource)).BeginInit();
            this.groupBox1.SuspendLayout();
            this.SuspendLayout();
            // 
            // booksDataSet
            // 
            this.booksDataSet.DataSetName = "BooksDataSet";
            this.booksDataSet.SchemaSerializationMode = System.Data.SchemaSerializationMode.IncludeSchema;
            // 
            // booksBindingSource
            // 
            this.booksBindingSource.DataMember = "Books";
            this.booksBindingSource.DataSource = this.booksDataSet;
            // 
            // booksTableAdapter
            // 
            this.booksTableAdapter.ClearBeforeFill = true;
            // 
            // tableAdapterManager
            // 
            this.tableAdapterManager.BackupDataSetBeforeUpdate = false;
            this.tableAdapterManager.BooksTableAdapter = this.booksTableAdapter;
            this.tableAdapterManager.UpdateOrder = vp_proektna_biblioteka.BooksDataSetTableAdapters.TableAdapterManager.UpdateOrderOption.InsertUpdateDelete;
            // 
            // btnDodadiZacuvaj
            // 
            this.btnDodadiZacuvaj.DialogResult = System.Windows.Forms.DialogResult.OK;
            this.btnDodadiZacuvaj.Location = new System.Drawing.Point(104, 122);
            this.btnDodadiZacuvaj.Name = "btnDodadiZacuvaj";
            this.btnDodadiZacuvaj.Size = new System.Drawing.Size(94, 23);
            this.btnDodadiZacuvaj.TabIndex = 9;
            this.btnDodadiZacuvaj.Text = "Додади";
            this.btnDodadiZacuvaj.UseVisualStyleBackColor = true;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(23, 19);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(48, 13);
            this.label1.TabIndex = 10;
            this.label1.Text = "Наслов:";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(31, 50);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(40, 13);
            this.label2.TabIndex = 11;
            this.label2.Text = "Автор:";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(6, 84);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(65, 13);
            this.label3.TabIndex = 12;
            this.label3.Text = "Категорија:";
            // 
            // groupBox1
            // 
            this.groupBox1.Controls.Add(this.tbKategorija);
            this.groupBox1.Controls.Add(this.btnDodadiZacuvaj);
            this.groupBox1.Controls.Add(this.tbAvtor);
            this.groupBox1.Controls.Add(this.tbNaslov);
            this.groupBox1.Controls.Add(this.label1);
            this.groupBox1.Controls.Add(this.label3);
            this.groupBox1.Controls.Add(this.label2);
            this.groupBox1.Location = new System.Drawing.Point(12, 12);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Size = new System.Drawing.Size(207, 151);
            this.groupBox1.TabIndex = 13;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "Додади книга";
            // 
            // tbKategorija
            // 
            this.tbKategorija.Location = new System.Drawing.Point(77, 81);
            this.tbKategorija.Name = "tbKategorija";
            this.tbKategorija.Size = new System.Drawing.Size(121, 20);
            this.tbKategorija.TabIndex = 15;
            // 
            // tbAvtor
            // 
            this.tbAvtor.Location = new System.Drawing.Point(77, 50);
            this.tbAvtor.Name = "tbAvtor";
            this.tbAvtor.Size = new System.Drawing.Size(121, 20);
            this.tbAvtor.TabIndex = 14;
            // 
            // tbNaslov
            // 
            this.tbNaslov.Location = new System.Drawing.Point(77, 19);
            this.tbNaslov.Name = "tbNaslov";
            this.tbNaslov.Size = new System.Drawing.Size(121, 20);
            this.tbNaslov.TabIndex = 13;
            // 
            // AddBook
            // 
            this.AcceptButton = this.btnDodadiZacuvaj;
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(235, 187);
            this.Controls.Add(this.groupBox1);
            this.Name = "AddBook";
            this.Text = "AddBook";
            this.Load += new System.EventHandler(this.AddBook_Load);
            ((System.ComponentModel.ISupportInitialize)(this.booksDataSet)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.booksBindingSource)).EndInit();
            this.groupBox1.ResumeLayout(false);
            this.groupBox1.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private BooksDataSet booksDataSet;
        private System.Windows.Forms.BindingSource booksBindingSource;
        private BooksDataSetTableAdapters.BooksTableAdapter booksTableAdapter;
        private BooksDataSetTableAdapters.TableAdapterManager tableAdapterManager;
        private System.Windows.Forms.Button btnDodadiZacuvaj;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.TextBox tbKategorija;
        private System.Windows.Forms.TextBox tbAvtor;
        private System.Windows.Forms.TextBox tbNaslov;

    }
}
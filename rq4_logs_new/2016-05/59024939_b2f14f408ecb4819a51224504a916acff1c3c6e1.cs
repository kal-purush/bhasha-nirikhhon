namespace ProjecteFinal
{
    partial class frmMDIPare
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
            this.mnuStripMDI = new System.Windows.Forms.MenuStrip();
            this.arxiuToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.tsmObtenirDades = new System.Windows.Forms.ToolStripMenuItem();
            this.tsmGenerarDades = new System.Windows.Forms.ToolStripMenuItem();
            this.tsmSortir = new System.Windows.Forms.ToolStripMenuItem();
            this.formularisToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.tsmClients = new System.Windows.Forms.ToolStripMenuItem();
            this.tsmArticles = new System.Windows.Forms.ToolStripMenuItem();
            this.tsmAlbarans = new System.Windows.Forms.ToolStripMenuItem();
            this.tsmFactures = new System.Windows.Forms.ToolStripMenuItem();
            this.tsmInformes = new System.Windows.Forms.ToolStripMenuItem();
            this.oracleDataSet = new ProjecteFinal.OracleDataSet();
            this.cLIENTSTableAdapter = new ProjecteFinal.OracleDataSetTableAdapters.CLIENTSTableAdapter();
            this.tableAdapterManager = new ProjecteFinal.OracleDataSetTableAdapters.TableAdapterManager();
            this.aRTICLESTableAdapter = new ProjecteFinal.OracleDataSetTableAdapters.ARTICLESTableAdapter();
            this.cABALBARATableAdapter = new ProjecteFinal.OracleDataSetTableAdapters.CABALBARATableAdapter();
            this.cABFACTURASTableAdapter = new ProjecteFinal.OracleDataSetTableAdapters.CABFACTURASTableAdapter();
            this.lINEASALBARATableAdapter = new ProjecteFinal.OracleDataSetTableAdapters.LINEASALBARATableAdapter();
            this.lINEASFACTURATableAdapter = new ProjecteFinal.OracleDataSetTableAdapters.LINEASFACTURATableAdapter();
            this.mUNICIPISTableAdapter = new ProjecteFinal.OracleDataSetTableAdapters.MUNICIPISTableAdapter();
            this.pROVINCIESTableAdapter = new ProjecteFinal.OracleDataSetTableAdapters.PROVINCIESTableAdapter();
            this.mnuStripMDI.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.oracleDataSet)).BeginInit();
            this.SuspendLayout();
            // 
            // mnuStripMDI
            // 
            this.mnuStripMDI.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.arxiuToolStripMenuItem,
            this.formularisToolStripMenuItem});
            this.mnuStripMDI.Location = new System.Drawing.Point(0, 0);
            this.mnuStripMDI.Name = "mnuStripMDI";
            this.mnuStripMDI.Size = new System.Drawing.Size(762, 24);
            this.mnuStripMDI.TabIndex = 0;
            this.mnuStripMDI.Text = "menuStrip1";
            // 
            // arxiuToolStripMenuItem
            // 
            this.arxiuToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.tsmObtenirDades,
            this.tsmGenerarDades,
            this.tsmSortir});
            this.arxiuToolStripMenuItem.Name = "arxiuToolStripMenuItem";
            this.arxiuToolStripMenuItem.Size = new System.Drawing.Size(46, 20);
            this.arxiuToolStripMenuItem.Text = "Arxiu";
            // 
            // tsmObtenirDades
            // 
            this.tsmObtenirDades.Name = "tsmObtenirDades";
            this.tsmObtenirDades.Size = new System.Drawing.Size(150, 22);
            this.tsmObtenirDades.Text = "Obtenir Dades";
            this.tsmObtenirDades.Click += new System.EventHandler(this.tsmObtenirDades_Click);
            // 
            // tsmGenerarDades
            // 
            this.tsmGenerarDades.Name = "tsmGenerarDades";
            this.tsmGenerarDades.Size = new System.Drawing.Size(150, 22);
            this.tsmGenerarDades.Text = "Generar Dades";
            this.tsmGenerarDades.Click += new System.EventHandler(this.tsmGenerarDades_Click);
            // 
            // tsmSortir
            // 
            this.tsmSortir.Name = "tsmSortir";
            this.tsmSortir.Size = new System.Drawing.Size(150, 22);
            this.tsmSortir.Text = "Sortir";
            this.tsmSortir.Click += new System.EventHandler(this.tsmSortir_Click);
            // 
            // formularisToolStripMenuItem
            // 
            this.formularisToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.tsmClients,
            this.tsmArticles,
            this.tsmAlbarans,
            this.tsmFactures,
            this.tsmInformes});
            this.formularisToolStripMenuItem.Name = "formularisToolStripMenuItem";
            this.formularisToolStripMenuItem.Size = new System.Drawing.Size(75, 20);
            this.formularisToolStripMenuItem.Text = "Formularis";
            // 
            // tsmClients
            // 
            this.tsmClients.Name = "tsmClients";
            this.tsmClients.Size = new System.Drawing.Size(121, 22);
            this.tsmClients.Text = "Clients";
            this.tsmClients.Click += new System.EventHandler(this.tsmClients_Click);
            // 
            // tsmArticles
            // 
            this.tsmArticles.Name = "tsmArticles";
            this.tsmArticles.Size = new System.Drawing.Size(121, 22);
            this.tsmArticles.Text = "Articles";
            this.tsmArticles.Click += new System.EventHandler(this.tsmArticles_Click);
            // 
            // tsmAlbarans
            // 
            this.tsmAlbarans.Name = "tsmAlbarans";
            this.tsmAlbarans.Size = new System.Drawing.Size(121, 22);
            this.tsmAlbarans.Text = "Albarans";
            this.tsmAlbarans.Click += new System.EventHandler(this.tsmAlbarans_Click);
            // 
            // tsmFactures
            // 
            this.tsmFactures.Name = "tsmFactures";
            this.tsmFactures.Size = new System.Drawing.Size(121, 22);
            this.tsmFactures.Text = "Factures";
            this.tsmFactures.Click += new System.EventHandler(this.tsmFactures_Click);
            // 
            // tsmInformes
            // 
            this.tsmInformes.Name = "tsmInformes";
            this.tsmInformes.Size = new System.Drawing.Size(121, 22);
            this.tsmInformes.Text = "Informes";
            this.tsmInformes.Click += new System.EventHandler(this.tsmInformes_Click);
            // 
            // oracleDataSet
            // 
            this.oracleDataSet.DataSetName = "OracleDataSet";
            this.oracleDataSet.SchemaSerializationMode = System.Data.SchemaSerializationMode.IncludeSchema;
            // 
            // cLIENTSTableAdapter
            // 
            this.cLIENTSTableAdapter.ClearBeforeFill = true;
            // 
            // tableAdapterManager
            // 
            this.tableAdapterManager.ARTICLESTableAdapter = this.aRTICLESTableAdapter;
            this.tableAdapterManager.BackupDataSetBeforeUpdate = false;
            this.tableAdapterManager.CABALBARATableAdapter = this.cABALBARATableAdapter;
            this.tableAdapterManager.CABFACTURASTableAdapter = this.cABFACTURASTableAdapter;
            this.tableAdapterManager.CLIENTSTableAdapter = this.cLIENTSTableAdapter;
            this.tableAdapterManager.LINEASALBARATableAdapter = this.lINEASALBARATableAdapter;
            this.tableAdapterManager.LINEASFACTURATableAdapter = this.lINEASFACTURATableAdapter;
            this.tableAdapterManager.MUNICIPISTableAdapter = this.mUNICIPISTableAdapter;
            this.tableAdapterManager.PROVINCIESTableAdapter = this.pROVINCIESTableAdapter;
            this.tableAdapterManager.UpdateOrder = ProjecteFinal.OracleDataSetTableAdapters.TableAdapterManager.UpdateOrderOption.InsertUpdateDelete;
            // 
            // aRTICLESTableAdapter
            // 
            this.aRTICLESTableAdapter.ClearBeforeFill = true;
            // 
            // cABALBARATableAdapter
            // 
            this.cABALBARATableAdapter.ClearBeforeFill = true;
            // 
            // cABFACTURASTableAdapter
            // 
            this.cABFACTURASTableAdapter.ClearBeforeFill = true;
            // 
            // lINEASALBARATableAdapter
            // 
            this.lINEASALBARATableAdapter.ClearBeforeFill = true;
            // 
            // lINEASFACTURATableAdapter
            // 
            this.lINEASFACTURATableAdapter.ClearBeforeFill = true;
            // 
            // mUNICIPISTableAdapter
            // 
            this.mUNICIPISTableAdapter.ClearBeforeFill = true;
            // 
            // pROVINCIESTableAdapter
            // 
            this.pROVINCIESTableAdapter.ClearBeforeFill = true;
            // 
            // frmMDIPare
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(762, 483);
            this.Controls.Add(this.mnuStripMDI);
            this.IsMdiContainer = true;
            this.MainMenuStrip = this.mnuStripMDI;
            this.Name = "frmMDIPare";
            this.Text = "MDI Pare";
            this.mnuStripMDI.ResumeLayout(false);
            this.mnuStripMDI.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.oracleDataSet)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.MenuStrip mnuStripMDI;
        private System.Windows.Forms.ToolStripMenuItem arxiuToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem tsmObtenirDades;
        private System.Windows.Forms.ToolStripMenuItem tsmGenerarDades;
        private System.Windows.Forms.ToolStripMenuItem tsmSortir;
        private System.Windows.Forms.ToolStripMenuItem formularisToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem tsmClients;
        private System.Windows.Forms.ToolStripMenuItem tsmArticles;
        private System.Windows.Forms.ToolStripMenuItem tsmAlbarans;
        private System.Windows.Forms.ToolStripMenuItem tsmFactures;
        private System.Windows.Forms.ToolStripMenuItem tsmInformes;
        private OracleDataSet oracleDataSet;
        private OracleDataSetTableAdapters.CLIENTSTableAdapter cLIENTSTableAdapter;
        private OracleDataSetTableAdapters.TableAdapterManager tableAdapterManager;
        private OracleDataSetTableAdapters.MUNICIPISTableAdapter mUNICIPISTableAdapter;
        private OracleDataSetTableAdapters.PROVINCIESTableAdapter pROVINCIESTableAdapter;
        private OracleDataSetTableAdapters.ARTICLESTableAdapter aRTICLESTableAdapter;
        private OracleDataSetTableAdapters.CABALBARATableAdapter cABALBARATableAdapter;
        private OracleDataSetTableAdapters.CABFACTURASTableAdapter cABFACTURASTableAdapter;
        private OracleDataSetTableAdapters.LINEASALBARATableAdapter lINEASALBARATableAdapter;
        private OracleDataSetTableAdapters.LINEASFACTURATableAdapter lINEASFACTURATableAdapter;
    }
}
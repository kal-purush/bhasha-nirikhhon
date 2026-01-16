namespace Przychodnia
{
    partial class Form1
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
            this.menuStrip1 = new System.Windows.Forms.MenuStrip();
            this.dodajDoBazyToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.lekarzaToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.wizytęToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.pacjentaToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.comboBox1 = new System.Windows.Forms.ComboBox();
            this.label1 = new System.Windows.Forms.Label();
            this.groupBox2 = new System.Windows.Forms.GroupBox();
            this.dataGridView1 = new System.Windows.Forms.DataGridView();
            this.iDDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.imieDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.nazwiskoDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.adresDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.telefonDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.dataUrodzeniaDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.idSpecjalizacjiDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.lekarzBindingSource = new System.Windows.Forms.BindingSource(this.components);
            this.bazaPrzychodniaDataSet = new Przychodnia.bazaPrzychodniaDataSet();
            this.lekarzTableAdapter = new Przychodnia.bazaPrzychodniaDataSetTableAdapters.LekarzTableAdapter();
            this.contextMenuStrip1 = new System.Windows.Forms.ContextMenuStrip(this.components);
            this.usuńToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.dataGridViewPacjenci = new System.Windows.Forms.DataGridView();
            this.dataGridViewTextBoxColumn1 = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.dataGridViewTextBoxColumn2 = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.dataGridViewTextBoxColumn3 = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.dataGridViewTextBoxColumn4 = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.dataGridViewTextBoxColumn5 = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.dataGridViewTextBoxColumn6 = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.pacjentBindingSource = new System.Windows.Forms.BindingSource(this.components);
            this.pacjentTableAdapter = new Przychodnia.bazaPrzychodniaDataSetTableAdapters.PacjentTableAdapter();
            this.dataGridViewSpecjalizacja = new System.Windows.Forms.DataGridView();
            this.specjalizacjaBindingSource = new System.Windows.Forms.BindingSource(this.components);
            this.specjalizacjaTableAdapter = new Przychodnia.bazaPrzychodniaDataSetTableAdapters.SpecjalizacjaTableAdapter();
            this.dataGridViewWizyta = new System.Windows.Forms.DataGridView();
            this.wizytaBindingSource = new System.Windows.Forms.BindingSource(this.components);
            this.wizytaTableAdapter = new Przychodnia.bazaPrzychodniaDataSetTableAdapters.WizytaTableAdapter();
            this.iDDataGridViewTextBoxColumn1 = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.idLekarzaDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.idPacjentaDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.dataWizytyDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.opisDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.iDDataGridViewTextBoxColumn2 = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.nazwaSpecjalizacjiDataGridViewTextBoxColumn = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.menuStrip1.SuspendLayout();
            this.groupBox1.SuspendLayout();
            this.groupBox2.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.dataGridView1)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.lekarzBindingSource)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.bazaPrzychodniaDataSet)).BeginInit();
            this.contextMenuStrip1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.dataGridViewPacjenci)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.pacjentBindingSource)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.dataGridViewSpecjalizacja)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.specjalizacjaBindingSource)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.dataGridViewWizyta)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.wizytaBindingSource)).BeginInit();
            this.SuspendLayout();
            // 
            // menuStrip1
            // 
            this.menuStrip1.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.dodajDoBazyToolStripMenuItem});
            this.menuStrip1.Location = new System.Drawing.Point(0, 0);
            this.menuStrip1.Name = "menuStrip1";
            this.menuStrip1.Padding = new System.Windows.Forms.Padding(8, 2, 0, 2);
            this.menuStrip1.Size = new System.Drawing.Size(836, 24);
            this.menuStrip1.TabIndex = 0;
            this.menuStrip1.Text = "menuStrip1";
            // 
            // dodajDoBazyToolStripMenuItem
            // 
            this.dodajDoBazyToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.lekarzaToolStripMenuItem,
            this.wizytęToolStripMenuItem,
            this.pacjentaToolStripMenuItem});
            this.dodajDoBazyToolStripMenuItem.Name = "dodajDoBazyToolStripMenuItem";
            this.dodajDoBazyToolStripMenuItem.Size = new System.Drawing.Size(94, 20);
            this.dodajDoBazyToolStripMenuItem.Text = "Dodaj do bazy";
            // 
            // lekarzaToolStripMenuItem
            // 
            this.lekarzaToolStripMenuItem.Name = "lekarzaToolStripMenuItem";
            this.lekarzaToolStripMenuItem.Size = new System.Drawing.Size(179, 22);
            this.lekarzaToolStripMenuItem.Text = "Lekarza (CTRL + L)";
            this.lekarzaToolStripMenuItem.Click += new System.EventHandler(this.lekarzaToolStripMenuItem_Click);
            // 
            // wizytęToolStripMenuItem
            // 
            this.wizytęToolStripMenuItem.Name = "wizytęToolStripMenuItem";
            this.wizytęToolStripMenuItem.Size = new System.Drawing.Size(179, 22);
            this.wizytęToolStripMenuItem.Text = "Wizytę (CTRL + W)";
            this.wizytęToolStripMenuItem.Click += new System.EventHandler(this.wizytęToolStripMenuItem_Click);
            // 
            // pacjentaToolStripMenuItem
            // 
            this.pacjentaToolStripMenuItem.Name = "pacjentaToolStripMenuItem";
            this.pacjentaToolStripMenuItem.Size = new System.Drawing.Size(179, 22);
            this.pacjentaToolStripMenuItem.Text = "Pacjenta (CTRL + P)";
            this.pacjentaToolStripMenuItem.Click += new System.EventHandler(this.pacjentaToolStripMenuItem_Click);
            // 
            // groupBox1
            // 
            this.groupBox1.Controls.Add(this.comboBox1);
            this.groupBox1.Controls.Add(this.label1);
            this.groupBox1.Dock = System.Windows.Forms.DockStyle.Top;
            this.groupBox1.Location = new System.Drawing.Point(0, 24);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Size = new System.Drawing.Size(836, 100);
            this.groupBox1.TabIndex = 1;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "Wybierz tabelę";
            // 
            // comboBox1
            // 
            this.comboBox1.FormattingEnabled = true;
            this.comboBox1.Items.AddRange(new object[] {
            "Lekarze",
            "Pacjenci",
            "Specjalizacja",
            "Wizyta"});
            this.comboBox1.Location = new System.Drawing.Point(468, 39);
            this.comboBox1.Name = "comboBox1";
            this.comboBox1.Size = new System.Drawing.Size(292, 24);
            this.comboBox1.TabIndex = 1;
            this.comboBox1.SelectedIndexChanged += new System.EventHandler(this.comboBox1_SelectedIndexChanged);
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(230, 42);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(98, 16);
            this.label1.TabIndex = 0;
            this.label1.Text = "Wybierz tabelę";
            // 
            // groupBox2
            // 
            this.groupBox2.Controls.Add(this.dataGridViewWizyta);
            this.groupBox2.Controls.Add(this.dataGridViewSpecjalizacja);
            this.groupBox2.Controls.Add(this.dataGridViewPacjenci);
            this.groupBox2.Controls.Add(this.dataGridView1);
            this.groupBox2.Dock = System.Windows.Forms.DockStyle.Fill;
            this.groupBox2.Location = new System.Drawing.Point(0, 124);
            this.groupBox2.Name = "groupBox2";
            this.groupBox2.Size = new System.Drawing.Size(836, 261);
            this.groupBox2.TabIndex = 2;
            this.groupBox2.TabStop = false;
            this.groupBox2.Text = "Dane";
            // 
            // dataGridView1
            // 
            this.dataGridView1.AllowUserToAddRows = false;
            this.dataGridView1.AutoGenerateColumns = false;
            this.dataGridView1.AutoSizeColumnsMode = System.Windows.Forms.DataGridViewAutoSizeColumnsMode.Fill;
            this.dataGridView1.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dataGridView1.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.iDDataGridViewTextBoxColumn,
            this.imieDataGridViewTextBoxColumn,
            this.nazwiskoDataGridViewTextBoxColumn,
            this.adresDataGridViewTextBoxColumn,
            this.telefonDataGridViewTextBoxColumn,
            this.dataUrodzeniaDataGridViewTextBoxColumn,
            this.idSpecjalizacjiDataGridViewTextBoxColumn});
            this.dataGridView1.ContextMenuStrip = this.contextMenuStrip1;
            this.dataGridView1.DataSource = this.lekarzBindingSource;
            this.dataGridView1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.dataGridView1.Location = new System.Drawing.Point(3, 18);
            this.dataGridView1.Name = "dataGridView1";
            this.dataGridView1.SelectionMode = System.Windows.Forms.DataGridViewSelectionMode.FullRowSelect;
            this.dataGridView1.Size = new System.Drawing.Size(830, 240);
            this.dataGridView1.TabIndex = 0;
            // 
            // iDDataGridViewTextBoxColumn
            // 
            this.iDDataGridViewTextBoxColumn.DataPropertyName = "ID";
            this.iDDataGridViewTextBoxColumn.HeaderText = "ID";
            this.iDDataGridViewTextBoxColumn.Name = "iDDataGridViewTextBoxColumn";
            this.iDDataGridViewTextBoxColumn.ReadOnly = true;
            // 
            // imieDataGridViewTextBoxColumn
            // 
            this.imieDataGridViewTextBoxColumn.DataPropertyName = "imie";
            this.imieDataGridViewTextBoxColumn.HeaderText = "imie";
            this.imieDataGridViewTextBoxColumn.Name = "imieDataGridViewTextBoxColumn";
            // 
            // nazwiskoDataGridViewTextBoxColumn
            // 
            this.nazwiskoDataGridViewTextBoxColumn.DataPropertyName = "nazwisko";
            this.nazwiskoDataGridViewTextBoxColumn.HeaderText = "nazwisko";
            this.nazwiskoDataGridViewTextBoxColumn.Name = "nazwiskoDataGridViewTextBoxColumn";
            // 
            // adresDataGridViewTextBoxColumn
            // 
            this.adresDataGridViewTextBoxColumn.DataPropertyName = "adres";
            this.adresDataGridViewTextBoxColumn.HeaderText = "adres";
            this.adresDataGridViewTextBoxColumn.Name = "adresDataGridViewTextBoxColumn";
            // 
            // telefonDataGridViewTextBoxColumn
            // 
            this.telefonDataGridViewTextBoxColumn.DataPropertyName = "telefon";
            this.telefonDataGridViewTextBoxColumn.HeaderText = "telefon";
            this.telefonDataGridViewTextBoxColumn.Name = "telefonDataGridViewTextBoxColumn";
            // 
            // dataUrodzeniaDataGridViewTextBoxColumn
            // 
            this.dataUrodzeniaDataGridViewTextBoxColumn.DataPropertyName = "dataUrodzenia";
            this.dataUrodzeniaDataGridViewTextBoxColumn.HeaderText = "dataUrodzenia";
            this.dataUrodzeniaDataGridViewTextBoxColumn.Name = "dataUrodzeniaDataGridViewTextBoxColumn";
            // 
            // idSpecjalizacjiDataGridViewTextBoxColumn
            // 
            this.idSpecjalizacjiDataGridViewTextBoxColumn.DataPropertyName = "idSpecjalizacji";
            this.idSpecjalizacjiDataGridViewTextBoxColumn.HeaderText = "idSpecjalizacji";
            this.idSpecjalizacjiDataGridViewTextBoxColumn.Name = "idSpecjalizacjiDataGridViewTextBoxColumn";
            // 
            // lekarzBindingSource
            // 
            this.lekarzBindingSource.DataMember = "Lekarz";
            this.lekarzBindingSource.DataSource = this.bazaPrzychodniaDataSet;
            // 
            // bazaPrzychodniaDataSet
            // 
            this.bazaPrzychodniaDataSet.DataSetName = "bazaPrzychodniaDataSet";
            this.bazaPrzychodniaDataSet.SchemaSerializationMode = System.Data.SchemaSerializationMode.IncludeSchema;
            // 
            // lekarzTableAdapter
            // 
            this.lekarzTableAdapter.ClearBeforeFill = true;
            // 
            // contextMenuStrip1
            // 
            this.contextMenuStrip1.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.usuńToolStripMenuItem});
            this.contextMenuStrip1.Name = "contextMenuStrip1";
            this.contextMenuStrip1.Size = new System.Drawing.Size(102, 26);
            // 
            // usuńToolStripMenuItem
            // 
            this.usuńToolStripMenuItem.Name = "usuńToolStripMenuItem";
            this.usuńToolStripMenuItem.Size = new System.Drawing.Size(101, 22);
            this.usuńToolStripMenuItem.Text = "Usuń";
            this.usuńToolStripMenuItem.Click += new System.EventHandler(this.usuńToolStripMenuItem_Click);
            // 
            // dataGridViewPacjenci
            // 
            this.dataGridViewPacjenci.AllowUserToAddRows = false;
            this.dataGridViewPacjenci.AutoGenerateColumns = false;
            this.dataGridViewPacjenci.AutoSizeColumnsMode = System.Windows.Forms.DataGridViewAutoSizeColumnsMode.Fill;
            this.dataGridViewPacjenci.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dataGridViewPacjenci.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.dataGridViewTextBoxColumn1,
            this.dataGridViewTextBoxColumn2,
            this.dataGridViewTextBoxColumn3,
            this.dataGridViewTextBoxColumn4,
            this.dataGridViewTextBoxColumn5,
            this.dataGridViewTextBoxColumn6});
            this.dataGridViewPacjenci.ContextMenuStrip = this.contextMenuStrip1;
            this.dataGridViewPacjenci.DataSource = this.pacjentBindingSource;
            this.dataGridViewPacjenci.Dock = System.Windows.Forms.DockStyle.Fill;
            this.dataGridViewPacjenci.Location = new System.Drawing.Point(3, 18);
            this.dataGridViewPacjenci.Name = "dataGridViewPacjenci";
            this.dataGridViewPacjenci.SelectionMode = System.Windows.Forms.DataGridViewSelectionMode.FullRowSelect;
            this.dataGridViewPacjenci.Size = new System.Drawing.Size(830, 240);
            this.dataGridViewPacjenci.TabIndex = 1;
            // 
            // dataGridViewTextBoxColumn1
            // 
            this.dataGridViewTextBoxColumn1.DataPropertyName = "ID";
            this.dataGridViewTextBoxColumn1.HeaderText = "ID";
            this.dataGridViewTextBoxColumn1.Name = "dataGridViewTextBoxColumn1";
            this.dataGridViewTextBoxColumn1.ReadOnly = true;
            // 
            // dataGridViewTextBoxColumn2
            // 
            this.dataGridViewTextBoxColumn2.DataPropertyName = "imie";
            this.dataGridViewTextBoxColumn2.HeaderText = "imie";
            this.dataGridViewTextBoxColumn2.Name = "dataGridViewTextBoxColumn2";
            // 
            // dataGridViewTextBoxColumn3
            // 
            this.dataGridViewTextBoxColumn3.DataPropertyName = "nazwisko";
            this.dataGridViewTextBoxColumn3.HeaderText = "nazwisko";
            this.dataGridViewTextBoxColumn3.Name = "dataGridViewTextBoxColumn3";
            // 
            // dataGridViewTextBoxColumn4
            // 
            this.dataGridViewTextBoxColumn4.DataPropertyName = "adres";
            this.dataGridViewTextBoxColumn4.HeaderText = "adres";
            this.dataGridViewTextBoxColumn4.Name = "dataGridViewTextBoxColumn4";
            // 
            // dataGridViewTextBoxColumn5
            // 
            this.dataGridViewTextBoxColumn5.DataPropertyName = "telefon";
            this.dataGridViewTextBoxColumn5.HeaderText = "telefon";
            this.dataGridViewTextBoxColumn5.Name = "dataGridViewTextBoxColumn5";
            // 
            // dataGridViewTextBoxColumn6
            // 
            this.dataGridViewTextBoxColumn6.DataPropertyName = "dataUrodzenia";
            this.dataGridViewTextBoxColumn6.HeaderText = "dataUrodzenia";
            this.dataGridViewTextBoxColumn6.Name = "dataGridViewTextBoxColumn6";
            // 
            // pacjentBindingSource
            // 
            this.pacjentBindingSource.DataMember = "Pacjent";
            this.pacjentBindingSource.DataSource = this.bazaPrzychodniaDataSet;
            // 
            // pacjentTableAdapter
            // 
            this.pacjentTableAdapter.ClearBeforeFill = true;
            // 
            // dataGridViewSpecjalizacja
            // 
            this.dataGridViewSpecjalizacja.AllowUserToAddRows = false;
            this.dataGridViewSpecjalizacja.AutoGenerateColumns = false;
            this.dataGridViewSpecjalizacja.AutoSizeColumnsMode = System.Windows.Forms.DataGridViewAutoSizeColumnsMode.Fill;
            this.dataGridViewSpecjalizacja.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dataGridViewSpecjalizacja.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.iDDataGridViewTextBoxColumn2,
            this.nazwaSpecjalizacjiDataGridViewTextBoxColumn});
            this.dataGridViewSpecjalizacja.ContextMenuStrip = this.contextMenuStrip1;
            this.dataGridViewSpecjalizacja.DataSource = this.specjalizacjaBindingSource;
            this.dataGridViewSpecjalizacja.Dock = System.Windows.Forms.DockStyle.Fill;
            this.dataGridViewSpecjalizacja.Location = new System.Drawing.Point(3, 18);
            this.dataGridViewSpecjalizacja.Name = "dataGridViewSpecjalizacja";
            this.dataGridViewSpecjalizacja.SelectionMode = System.Windows.Forms.DataGridViewSelectionMode.FullRowSelect;
            this.dataGridViewSpecjalizacja.Size = new System.Drawing.Size(830, 240);
            this.dataGridViewSpecjalizacja.TabIndex = 2;
            // 
            // specjalizacjaBindingSource
            // 
            this.specjalizacjaBindingSource.DataMember = "Specjalizacja";
            this.specjalizacjaBindingSource.DataSource = this.bazaPrzychodniaDataSet;
            // 
            // specjalizacjaTableAdapter
            // 
            this.specjalizacjaTableAdapter.ClearBeforeFill = true;
            // 
            // dataGridViewWizyta
            // 
            this.dataGridViewWizyta.AllowUserToAddRows = false;
            this.dataGridViewWizyta.AutoGenerateColumns = false;
            this.dataGridViewWizyta.AutoSizeColumnsMode = System.Windows.Forms.DataGridViewAutoSizeColumnsMode.Fill;
            this.dataGridViewWizyta.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dataGridViewWizyta.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.iDDataGridViewTextBoxColumn1,
            this.idLekarzaDataGridViewTextBoxColumn,
            this.idPacjentaDataGridViewTextBoxColumn,
            this.dataWizytyDataGridViewTextBoxColumn,
            this.opisDataGridViewTextBoxColumn});
            this.dataGridViewWizyta.ContextMenuStrip = this.contextMenuStrip1;
            this.dataGridViewWizyta.DataSource = this.wizytaBindingSource;
            this.dataGridViewWizyta.Dock = System.Windows.Forms.DockStyle.Fill;
            this.dataGridViewWizyta.Location = new System.Drawing.Point(3, 18);
            this.dataGridViewWizyta.Name = "dataGridViewWizyta";
            this.dataGridViewWizyta.SelectionMode = System.Windows.Forms.DataGridViewSelectionMode.FullRowSelect;
            this.dataGridViewWizyta.Size = new System.Drawing.Size(830, 240);
            this.dataGridViewWizyta.TabIndex = 3;
            // 
            // wizytaBindingSource
            // 
            this.wizytaBindingSource.DataMember = "Wizyta";
            this.wizytaBindingSource.DataSource = this.bazaPrzychodniaDataSet;
            // 
            // wizytaTableAdapter
            // 
            this.wizytaTableAdapter.ClearBeforeFill = true;
            // 
            // iDDataGridViewTextBoxColumn1
            // 
            this.iDDataGridViewTextBoxColumn1.DataPropertyName = "ID";
            this.iDDataGridViewTextBoxColumn1.HeaderText = "ID";
            this.iDDataGridViewTextBoxColumn1.Name = "iDDataGridViewTextBoxColumn1";
            this.iDDataGridViewTextBoxColumn1.ReadOnly = true;
            // 
            // idLekarzaDataGridViewTextBoxColumn
            // 
            this.idLekarzaDataGridViewTextBoxColumn.DataPropertyName = "idLekarza";
            this.idLekarzaDataGridViewTextBoxColumn.HeaderText = "idLekarza";
            this.idLekarzaDataGridViewTextBoxColumn.Name = "idLekarzaDataGridViewTextBoxColumn";
            // 
            // idPacjentaDataGridViewTextBoxColumn
            // 
            this.idPacjentaDataGridViewTextBoxColumn.DataPropertyName = "idPacjenta";
            this.idPacjentaDataGridViewTextBoxColumn.HeaderText = "idPacjenta";
            this.idPacjentaDataGridViewTextBoxColumn.Name = "idPacjentaDataGridViewTextBoxColumn";
            // 
            // dataWizytyDataGridViewTextBoxColumn
            // 
            this.dataWizytyDataGridViewTextBoxColumn.DataPropertyName = "dataWizyty";
            this.dataWizytyDataGridViewTextBoxColumn.HeaderText = "dataWizyty";
            this.dataWizytyDataGridViewTextBoxColumn.Name = "dataWizytyDataGridViewTextBoxColumn";
            // 
            // opisDataGridViewTextBoxColumn
            // 
            this.opisDataGridViewTextBoxColumn.DataPropertyName = "opis";
            this.opisDataGridViewTextBoxColumn.HeaderText = "opis";
            this.opisDataGridViewTextBoxColumn.Name = "opisDataGridViewTextBoxColumn";
            // 
            // iDDataGridViewTextBoxColumn2
            // 
            this.iDDataGridViewTextBoxColumn2.DataPropertyName = "ID";
            this.iDDataGridViewTextBoxColumn2.HeaderText = "ID";
            this.iDDataGridViewTextBoxColumn2.Name = "iDDataGridViewTextBoxColumn2";
            this.iDDataGridViewTextBoxColumn2.ReadOnly = true;
            // 
            // nazwaSpecjalizacjiDataGridViewTextBoxColumn
            // 
            this.nazwaSpecjalizacjiDataGridViewTextBoxColumn.DataPropertyName = "nazwaSpecjalizacji";
            this.nazwaSpecjalizacjiDataGridViewTextBoxColumn.HeaderText = "nazwaSpecjalizacji";
            this.nazwaSpecjalizacjiDataGridViewTextBoxColumn.Name = "nazwaSpecjalizacjiDataGridViewTextBoxColumn";
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.SystemColors.ControlLightLight;
            this.ClientSize = new System.Drawing.Size(836, 385);
            this.Controls.Add(this.groupBox2);
            this.Controls.Add(this.groupBox1);
            this.Controls.Add(this.menuStrip1);
            this.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(238)));
            this.MainMenuStrip = this.menuStrip1;
            this.Margin = new System.Windows.Forms.Padding(4);
            this.Name = "Form1";
            this.Text = "Baza przychodnia";
            this.Load += new System.EventHandler(this.Form1_Load);
            this.menuStrip1.ResumeLayout(false);
            this.menuStrip1.PerformLayout();
            this.groupBox1.ResumeLayout(false);
            this.groupBox1.PerformLayout();
            this.groupBox2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.dataGridView1)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.lekarzBindingSource)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.bazaPrzychodniaDataSet)).EndInit();
            this.contextMenuStrip1.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.dataGridViewPacjenci)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.pacjentBindingSource)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.dataGridViewSpecjalizacja)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.specjalizacjaBindingSource)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.dataGridViewWizyta)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.wizytaBindingSource)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.MenuStrip menuStrip1;
        private System.Windows.Forms.ToolStripMenuItem dodajDoBazyToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem lekarzaToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem wizytęToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem pacjentaToolStripMenuItem;
        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.GroupBox groupBox2;
        private System.Windows.Forms.DataGridView dataGridView1;
        private System.Windows.Forms.ComboBox comboBox1;
        private System.Windows.Forms.Label label1;
        private bazaPrzychodniaDataSet bazaPrzychodniaDataSet;
        private System.Windows.Forms.BindingSource lekarzBindingSource;
        private bazaPrzychodniaDataSetTableAdapters.LekarzTableAdapter lekarzTableAdapter;
        private System.Windows.Forms.DataGridViewTextBoxColumn iDDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn imieDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn nazwiskoDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn adresDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn telefonDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn dataUrodzeniaDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn idSpecjalizacjiDataGridViewTextBoxColumn;
        private System.Windows.Forms.ContextMenuStrip contextMenuStrip1;
        private System.Windows.Forms.ToolStripMenuItem usuńToolStripMenuItem;
        private System.Windows.Forms.DataGridView dataGridViewPacjenci;
        private System.Windows.Forms.DataGridViewTextBoxColumn dataGridViewTextBoxColumn1;
        private System.Windows.Forms.DataGridViewTextBoxColumn dataGridViewTextBoxColumn2;
        private System.Windows.Forms.DataGridViewTextBoxColumn dataGridViewTextBoxColumn3;
        private System.Windows.Forms.DataGridViewTextBoxColumn dataGridViewTextBoxColumn4;
        private System.Windows.Forms.DataGridViewTextBoxColumn dataGridViewTextBoxColumn5;
        private System.Windows.Forms.DataGridViewTextBoxColumn dataGridViewTextBoxColumn6;
        private System.Windows.Forms.BindingSource pacjentBindingSource;
        private bazaPrzychodniaDataSetTableAdapters.PacjentTableAdapter pacjentTableAdapter;
        private System.Windows.Forms.DataGridView dataGridViewSpecjalizacja;
        private System.Windows.Forms.BindingSource specjalizacjaBindingSource;
        private bazaPrzychodniaDataSetTableAdapters.SpecjalizacjaTableAdapter specjalizacjaTableAdapter;
        private System.Windows.Forms.DataGridView dataGridViewWizyta;
        private System.Windows.Forms.BindingSource wizytaBindingSource;
        private bazaPrzychodniaDataSetTableAdapters.WizytaTableAdapter wizytaTableAdapter;
        private System.Windows.Forms.DataGridViewTextBoxColumn iDDataGridViewTextBoxColumn1;
        private System.Windows.Forms.DataGridViewTextBoxColumn idLekarzaDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn idPacjentaDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn dataWizytyDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn opisDataGridViewTextBoxColumn;
        private System.Windows.Forms.DataGridViewTextBoxColumn iDDataGridViewTextBoxColumn2;
        private System.Windows.Forms.DataGridViewTextBoxColumn nazwaSpecjalizacjiDataGridViewTextBoxColumn;

    }
}

using System.Windows.Forms;

namespace Check_Disk
{
    partial class Form1
    {
        /// <summary>
        ///  Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        ///  Clean up any resources being used.
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
        ///  Required method for Designer support - do not modify
        ///  the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.zapisButton = new System.Windows.Forms.RadioButton();
            this.odczytButton = new System.Windows.Forms.RadioButton();
            this.zapisOdczytButton = new System.Windows.Forms.RadioButton();
            this.iloscPlikow = new System.Windows.Forms.NumericUpDown();
            this.TextIloscPlikow = new System.Windows.Forms.Label();
            this.zacznijTest = new System.Windows.Forms.Button();
            this.preDifiniowanyTest = new System.Windows.Forms.RadioButton();
            this.RozmiarPlikow = new System.Windows.Forms.NumericUpDown();
            this.TextPodajRozmiar = new System.Windows.Forms.Label();
            this.weryfikacja = new System.Windows.Forms.CheckBox();
            this.podajSciezke = new System.Windows.Forms.Button();
            this.wyswietlSciezke = new System.Windows.Forms.Label();
            this.TextMB = new System.Windows.Forms.Label();
            this.FileDialog = new System.Windows.Forms.OpenFileDialog();
            this.FolderDialog = new System.Windows.Forms.FolderBrowserDialog();
            this.PojemnikBledow = new System.Windows.Forms.Label();
            this.ZapiszConfig = new System.Windows.Forms.Button();
            this.LiczbaWatkowText = new System.Windows.Forms.Label();
            this.LiczbaWatkow = new System.Windows.Forms.NumericUpDown();
            this.ZapisStat = new System.Windows.Forms.Label();
            this.OdczytStaty = new System.Windows.Forms.Label();
            this.WczytajConfig = new System.Windows.Forms.Button();
            this.Kopiuj = new System.Windows.Forms.Button();
            this.Zgodnosc = new System.Windows.Forms.Label();
            ((System.ComponentModel.ISupportInitialize)(this.iloscPlikow)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.RozmiarPlikow)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.LiczbaWatkow)).BeginInit();
            this.SuspendLayout();
            // 
            // zapisButton
            // 
            this.zapisButton.AutoSize = true;
            this.zapisButton.Location = new System.Drawing.Point(12, 12);
            this.zapisButton.Name = "zapisButton";
            this.zapisButton.Size = new System.Drawing.Size(53, 19);
            this.zapisButton.TabIndex = 1;
            this.zapisButton.TabStop = true;
            this.zapisButton.Text = "Zapis";
            this.zapisButton.UseVisualStyleBackColor = true;
            // 
            // odczytButton
            // 
            this.odczytButton.AutoSize = true;
            this.odczytButton.Location = new System.Drawing.Point(12, 37);
            this.odczytButton.Name = "odczytButton";
            this.odczytButton.Size = new System.Drawing.Size(62, 19);
            this.odczytButton.TabIndex = 2;
            this.odczytButton.TabStop = true;
            this.odczytButton.Text = "Odczyt";
            this.odczytButton.UseVisualStyleBackColor = true;
            // 
            // zapisOdczytButton
            // 
            this.zapisOdczytButton.AutoSize = true;
            this.zapisOdczytButton.Location = new System.Drawing.Point(12, 62);
            this.zapisOdczytButton.Name = "zapisOdczytButton";
            this.zapisOdczytButton.Size = new System.Drawing.Size(95, 19);
            this.zapisOdczytButton.TabIndex = 3;
            this.zapisOdczytButton.TabStop = true;
            this.zapisOdczytButton.Text = "Zapis\\Odczyt";
            this.zapisOdczytButton.UseVisualStyleBackColor = true;
            // 
            // iloscPlikow
            // 
            this.iloscPlikow.Location = new System.Drawing.Point(495, 8);
            this.iloscPlikow.Maximum = new decimal(new int[] {
            50,
            0,
            0,
            0});
            this.iloscPlikow.Minimum = new decimal(new int[] {
            1,
            0,
            0,
            0});
            this.iloscPlikow.Name = "iloscPlikow";
            this.iloscPlikow.Size = new System.Drawing.Size(96, 23);
            this.iloscPlikow.TabIndex = 4;
            this.iloscPlikow.Value = new decimal(new int[] {
            1,
            0,
            0,
            0});
            // 
            // TextIloscPlikow
            // 
            this.TextIloscPlikow.AutoSize = true;
            this.TextIloscPlikow.Location = new System.Drawing.Point(408, 10);
            this.TextIloscPlikow.Name = "TextIloscPlikow";
            this.TextIloscPlikow.Size = new System.Drawing.Size(81, 15);
            this.TextIloscPlikow.TabIndex = 5;
            this.TextIloscPlikow.Text = "Liczba plików:";
            // 
            // zacznijTest
            // 
            this.zacznijTest.Location = new System.Drawing.Point(496, 372);
            this.zacznijTest.Name = "zacznijTest";
            this.zacznijTest.Size = new System.Drawing.Size(127, 23);
            this.zacznijTest.TabIndex = 6;
            this.zacznijTest.Text = "Rozpocznij test";
            this.zacznijTest.UseVisualStyleBackColor = true;
            this.zacznijTest.Click += new System.EventHandler(this.zacznijTest_Click);
            // 
            // preDifiniowanyTest
            // 
            this.preDifiniowanyTest.AutoSize = true;
            this.preDifiniowanyTest.Location = new System.Drawing.Point(12, 87);
            this.preDifiniowanyTest.Name = "preDifiniowanyTest";
            this.preDifiniowanyTest.Size = new System.Drawing.Size(129, 19);
            this.preDifiniowanyTest.TabIndex = 7;
            this.preDifiniowanyTest.TabStop = true;
            this.preDifiniowanyTest.Text = "Predefiniowany test";
            this.preDifiniowanyTest.UseVisualStyleBackColor = true;
            // 
            // RozmiarPlikow
            // 
            this.RozmiarPlikow.Location = new System.Drawing.Point(495, 37);
            this.RozmiarPlikow.Maximum = new decimal(new int[] {
            4096,
            0,
            0,
            0});
            this.RozmiarPlikow.Minimum = new decimal(new int[] {
            1,
            0,
            0,
            0});
            this.RozmiarPlikow.Name = "RozmiarPlikow";
            this.RozmiarPlikow.Size = new System.Drawing.Size(96, 23);
            this.RozmiarPlikow.TabIndex = 8;
            this.RozmiarPlikow.Value = new decimal(new int[] {
            50,
            0,
            0,
            0});
            // 
            // TextPodajRozmiar
            // 
            this.TextPodajRozmiar.AutoSize = true;
            this.TextPodajRozmiar.Location = new System.Drawing.Point(300, 39);
            this.TextPodajRozmiar.Name = "TextPodajRozmiar";
            this.TextPodajRozmiar.Size = new System.Drawing.Size(189, 15);
            this.TextPodajRozmiar.TabIndex = 9;
            this.TextPodajRozmiar.Text = "Podaj rozmiar pojedyńczego pliku:";
            // 
            // weryfikacja
            // 
            this.weryfikacja.AutoSize = true;
            this.weryfikacja.Location = new System.Drawing.Point(345, 87);
            this.weryfikacja.Name = "weryfikacja";
            this.weryfikacja.Size = new System.Drawing.Size(144, 19);
            this.weryfikacja.TabIndex = 10;
            this.weryfikacja.Text = "Weryfikacja zgodności";
            this.weryfikacja.UseVisualStyleBackColor = true;
            // 
            // podajSciezke
            // 
            this.podajSciezke.Location = new System.Drawing.Point(12, 123);
            this.podajSciezke.Name = "podajSciezke";
            this.podajSciezke.Size = new System.Drawing.Size(95, 23);
            this.podajSciezke.TabIndex = 11;
            this.podajSciezke.Text = "Podaj ścieżkę";
            this.podajSciezke.UseVisualStyleBackColor = true;
            this.podajSciezke.Click += new System.EventHandler(this.podajSciezke_Click);
            // 
            // wyswietlSciezke
            // 
            this.wyswietlSciezke.AutoSize = true;
            this.wyswietlSciezke.Location = new System.Drawing.Point(113, 127);
            this.wyswietlSciezke.Name = "wyswietlSciezke";
            this.wyswietlSciezke.Size = new System.Drawing.Size(0, 15);
            this.wyswietlSciezke.TabIndex = 12;
            // 
            // TextMB
            // 
            this.TextMB.AutoSize = true;
            this.TextMB.Location = new System.Drawing.Point(597, 39);
            this.TextMB.Name = "TextMB";
            this.TextMB.Size = new System.Drawing.Size(25, 15);
            this.TextMB.TabIndex = 13;
            this.TextMB.Text = "MB";
            // 
            // FileDialog
            // 
            this.FileDialog.CheckFileExists = false;
            this.FileDialog.Title = "Wybierz ścieżkę";
            this.FileDialog.ValidateNames = false;
            // 
            // PojemnikBledow
            // 
            this.PojemnikBledow.AutoSize = true;
            this.PojemnikBledow.Location = new System.Drawing.Point(12, 376);
            this.PojemnikBledow.Name = "PojemnikBledow";
            this.PojemnikBledow.Size = new System.Drawing.Size(0, 15);
            this.PojemnikBledow.TabIndex = 14;
            // 
            // ZapiszConfig
            // 
            this.ZapiszConfig.Location = new System.Drawing.Point(495, 314);
            this.ZapiszConfig.Name = "ZapiszConfig";
            this.ZapiszConfig.Size = new System.Drawing.Size(127, 23);
            this.ZapiszConfig.TabIndex = 15;
            this.ZapiszConfig.Text = "Zapisz konfiguracje";
            this.ZapiszConfig.UseVisualStyleBackColor = true;
            this.ZapiszConfig.Click += new System.EventHandler(this.ZapiszConfig_Click);
            // 
            // LiczbaWatkowText
            // 
            this.LiczbaWatkowText.AutoSize = true;
            this.LiczbaWatkowText.Location = new System.Drawing.Point(12, 171);
            this.LiczbaWatkowText.Name = "LiczbaWatkowText";
            this.LiczbaWatkowText.Size = new System.Drawing.Size(84, 15);
            this.LiczbaWatkowText.TabIndex = 17;
            this.LiczbaWatkowText.Text = "Liczba wątków";
            // 
            // LiczbaWatkow
            // 
            this.LiczbaWatkow.Location = new System.Drawing.Point(102, 169);
            this.LiczbaWatkow.Maximum = new decimal(new int[] {
            10000,
            0,
            0,
            0});
            this.LiczbaWatkow.Minimum = new decimal(new int[] {
            1,
            0,
            0,
            0});
            this.LiczbaWatkow.Name = "LiczbaWatkow";
            this.LiczbaWatkow.Size = new System.Drawing.Size(52, 23);
            this.LiczbaWatkow.TabIndex = 18;
            this.LiczbaWatkow.Value = new decimal(new int[] {
            1,
            0,
            0,
            0});
            // 
            // ZapisStat
            // 
            this.ZapisStat.AutoSize = true;
            this.ZapisStat.Location = new System.Drawing.Point(149, 224);
            this.ZapisStat.Name = "ZapisStat";
            this.ZapisStat.Size = new System.Drawing.Size(93, 15);
            this.ZapisStat.TabIndex = 19;
            this.ZapisStat.Text = "Zapis: 3500MB/s";
            this.ZapisStat.Visible = false;
            // 
            // OdczytStaty
            // 
            this.OdczytStaty.AutoSize = true;
            this.OdczytStaty.Location = new System.Drawing.Point(300, 224);
            this.OdczytStaty.Name = "OdczytStaty";
            this.OdczytStaty.Size = new System.Drawing.Size(102, 15);
            this.OdczytStaty.TabIndex = 20;
            this.OdczytStaty.Text = "Odczyt: 3000MB/s";
            this.OdczytStaty.Visible = false;
            // 
            // WczytajConfig
            // 
            this.WczytajConfig.Location = new System.Drawing.Point(495, 343);
            this.WczytajConfig.Name = "WczytajConfig";
            this.WczytajConfig.Size = new System.Drawing.Size(127, 23);
            this.WczytajConfig.TabIndex = 21;
            this.WczytajConfig.Text = "Wczytaj konfiguracje";
            this.WczytajConfig.UseVisualStyleBackColor = true;
            this.WczytajConfig.Click += new System.EventHandler(this.WczytajConfig_Click);
            // 
            // Kopiuj
            // 
            this.Kopiuj.Location = new System.Drawing.Point(216, 242);
            this.Kopiuj.Name = "Kopiuj";
            this.Kopiuj.Size = new System.Drawing.Size(104, 23);
            this.Kopiuj.TabIndex = 22;
            this.Kopiuj.Text = "Kopiuj wyniki";
            this.Kopiuj.UseVisualStyleBackColor = true;
            this.Kopiuj.Visible = false;
            this.Kopiuj.Click += new System.EventHandler(this.Kopiuj_Click);
            // 
            // Zgodnosc
            // 
            this.Zgodnosc.AutoSize = true;
            this.Zgodnosc.Location = new System.Drawing.Point(364, 109);
            this.Zgodnosc.Name = "Zgodnosc";
            this.Zgodnosc.Size = new System.Drawing.Size(38, 15);
            this.Zgodnosc.TabIndex = 23;
            this.Zgodnosc.Text = "label1";
            this.Zgodnosc.Visible = false;
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(7F, 15F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(635, 407);
            this.Controls.Add(this.Zgodnosc);
            this.Controls.Add(this.Kopiuj);
            this.Controls.Add(this.WczytajConfig);
            this.Controls.Add(this.OdczytStaty);
            this.Controls.Add(this.ZapisStat);
            this.Controls.Add(this.LiczbaWatkow);
            this.Controls.Add(this.LiczbaWatkowText);
            this.Controls.Add(this.ZapiszConfig);
            this.Controls.Add(this.PojemnikBledow);
            this.Controls.Add(this.TextMB);
            this.Controls.Add(this.wyswietlSciezke);
            this.Controls.Add(this.podajSciezke);
            this.Controls.Add(this.weryfikacja);
            this.Controls.Add(this.TextPodajRozmiar);
            this.Controls.Add(this.RozmiarPlikow);
            this.Controls.Add(this.preDifiniowanyTest);
            this.Controls.Add(this.zacznijTest);
            this.Controls.Add(this.TextIloscPlikow);
            this.Controls.Add(this.iloscPlikow);
            this.Controls.Add(this.zapisOdczytButton);
            this.Controls.Add(this.odczytButton);
            this.Controls.Add(this.zapisButton);
            this.Name = "Form1";
            this.Text = "CheckDisk";
            ((System.ComponentModel.ISupportInitialize)(this.iloscPlikow)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.RozmiarPlikow)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.LiczbaWatkow)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion
        public RadioButton zapisButton;
        public RadioButton odczytButton;
        public RadioButton zapisOdczytButton;
        public NumericUpDown iloscPlikow;
        public Label TextIloscPlikow;
        public Button zacznijTest;
        public RadioButton preDifiniowanyTest;
        public NumericUpDown RozmiarPlikow;
        public Label TextPodajRozmiar;
        public CheckBox weryfikacja;
        public Button podajSciezke;
        public Label wyswietlSciezke;
        public Label TextMB;
        public OpenFileDialog FileDialog;
        public FolderBrowserDialog FolderDialog;
        public Label PojemnikBledow;
        public Button ZapiszConfig;
        public Label LiczbaWatkowText;
        public NumericUpDown LiczbaWatkow;
        public Label ZapisStat;
        public Label OdczytStaty;
        public Button WczytajConfig;
        public Button Kopiuj;
        public Label Zgodnosc;

    }
}
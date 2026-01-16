namespace wypozyczalniaSamochodow
{
    partial class ReservationEditingPanel
    {
        /// <summary> 
        /// Wymagana zmienna projektanta.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary> 
        /// Wyczyść wszystkie używane zasoby.
        /// </summary>
        /// <param name="disposing">prawda, jeżeli zarządzane zasoby powinny zostać zlikwidowane; Fałsz w przeciwnym wypadku.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Kod wygenerowany przez Projektanta składników

        /// <summary> 
        /// Metoda wymagana do obsługi projektanta — nie należy modyfikować 
        /// jej zawartości w edytorze kodu.
        /// </summary>
        private void InitializeComponent()
        {
            this.goBackButton = new System.Windows.Forms.Button();
            this.dateFromPicker = new System.Windows.Forms.DateTimePicker();
            this.dateToPicker = new System.Windows.Forms.DateTimePicker();
            this.SuspendLayout();
            // 
            // goBackButton
            // 
            this.goBackButton.Location = new System.Drawing.Point(32, 86);
            this.goBackButton.Name = "goBackButton";
            this.goBackButton.Size = new System.Drawing.Size(109, 31);
            this.goBackButton.TabIndex = 0;
            this.goBackButton.Text = "Powrót";
            this.goBackButton.UseVisualStyleBackColor = true;
            this.goBackButton.Click += new System.EventHandler(this.goBackButton_Click);
            // 
            // dateFromPicker
            // 
            this.dateFromPicker.Location = new System.Drawing.Point(216, 86);
            this.dateFromPicker.Name = "dateFromPicker";
            this.dateFromPicker.Size = new System.Drawing.Size(200, 22);
            this.dateFromPicker.TabIndex = 1;
            this.dateFromPicker.ValueChanged += new System.EventHandler(this.dateFromPicker_ValueChanged);
            // 
            // dateToPicker
            // 
            this.dateToPicker.Location = new System.Drawing.Point(496, 86);
            this.dateToPicker.Name = "dateToPicker";
            this.dateToPicker.Size = new System.Drawing.Size(200, 22);
            this.dateToPicker.TabIndex = 2;
            this.dateToPicker.ValueChanged += new System.EventHandler(this.dateToPicker_ValueChanged);
            // 
            // ReservationEditingPanel
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.dateToPicker);
            this.Controls.Add(this.dateFromPicker);
            this.Controls.Add(this.goBackButton);
            this.Name = "ReservationEditingPanel";
            this.Size = new System.Drawing.Size(1004, 595);
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Button goBackButton;
        private System.Windows.Forms.DateTimePicker dateFromPicker;
        private System.Windows.Forms.DateTimePicker dateToPicker;
    }
}
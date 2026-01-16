
namespace TAB_clinic_GUI
{
    partial class VisitsForm
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
            this.dataVisits = new System.Windows.Forms.DataGridView();
            this.buttonSave = new System.Windows.Forms.Button();
            this.label1 = new System.Windows.Forms.Label();
            this.inputLastName = new System.Windows.Forms.TextBox();
            this.buttonSearch = new System.Windows.Forms.Button();
            this.label2 = new System.Windows.Forms.Label();
            this.inputDateFrom = new System.Windows.Forms.DateTimePicker();
            this.inputDateTo = new System.Windows.Forms.DateTimePicker();
            this.label3 = new System.Windows.Forms.Label();
            this.label4 = new System.Windows.Forms.Label();
            this.inputPesel = new System.Windows.Forms.TextBox();
            this.buttonClear = new System.Windows.Forms.Button();
            ((System.ComponentModel.ISupportInitialize)(this.dataVisits)).BeginInit();
            this.SuspendLayout();
            // 
            // dataVisits
            // 
            this.dataVisits.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dataVisits.Location = new System.Drawing.Point(14, 109);
            this.dataVisits.Margin = new System.Windows.Forms.Padding(5);
            this.dataVisits.Name = "dataVisits";
            this.dataVisits.RowHeadersWidth = 51;
            this.dataVisits.RowTemplate.Height = 29;
            this.dataVisits.Size = new System.Drawing.Size(1327, 470);
            this.dataVisits.TabIndex = 0;
            // 
            // buttonSave
            // 
            this.buttonSave.Location = new System.Drawing.Point(1006, 589);
            this.buttonSave.Margin = new System.Windows.Forms.Padding(5);
            this.buttonSave.Name = "buttonSave";
            this.buttonSave.Size = new System.Drawing.Size(335, 88);
            this.buttonSave.TabIndex = 4;
            this.buttonSave.Text = "Cancel Visit";
            this.buttonSave.UseVisualStyleBackColor = true;
            this.buttonSave.Click += new System.EventHandler(this.buttonSave_Click);
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(481, 62);
            this.label1.Margin = new System.Windows.Forms.Padding(5, 0, 5, 0);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(114, 32);
            this.label1.TabIndex = 9;
            this.label1.Text = "Surname:";
            // 
            // inputLastName
            // 
            this.inputLastName.Location = new System.Drawing.Point(605, 59);
            this.inputLastName.Margin = new System.Windows.Forms.Padding(5, 6, 5, 6);
            this.inputLastName.Name = "inputLastName";
            this.inputLastName.Size = new System.Drawing.Size(349, 39);
            this.inputLastName.TabIndex = 8;
            // 
            // buttonSearch
            // 
            this.buttonSearch.Location = new System.Drawing.Point(964, 57);
            this.buttonSearch.Margin = new System.Windows.Forms.Padding(5);
            this.buttonSearch.Name = "buttonSearch";
            this.buttonSearch.Size = new System.Drawing.Size(377, 41);
            this.buttonSearch.TabIndex = 7;
            this.buttonSearch.Text = "Search";
            this.buttonSearch.UseVisualStyleBackColor = true;
            this.buttonSearch.Click += new System.EventHandler(this.buttonSearch_Click);
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(14, 17);
            this.label2.Margin = new System.Windows.Forms.Padding(5, 0, 5, 0);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(127, 32);
            this.label2.TabIndex = 11;
            this.label2.Text = "Date from:";
            // 
            // inputDateFrom
            // 
            this.inputDateFrom.CustomFormat = "  MM/dd/yyyy  h:mm tt";
            this.inputDateFrom.Format = System.Windows.Forms.DateTimePickerFormat.Custom;
            this.inputDateFrom.Location = new System.Drawing.Point(149, 12);
            this.inputDateFrom.Name = "inputDateFrom";
            this.inputDateFrom.Size = new System.Drawing.Size(324, 39);
            this.inputDateFrom.TabIndex = 13;
            this.inputDateFrom.Value = new System.DateTime(2020, 1, 1, 0, 0, 0, 0);
            // 
            // inputDateTo
            // 
            this.inputDateTo.CustomFormat = "  MM/dd/yyyy  h:mm tt";
            this.inputDateTo.Format = System.Windows.Forms.DateTimePickerFormat.Custom;
            this.inputDateTo.Location = new System.Drawing.Point(149, 57);
            this.inputDateTo.Name = "inputDateTo";
            this.inputDateTo.Size = new System.Drawing.Size(324, 39);
            this.inputDateTo.TabIndex = 14;
            this.inputDateTo.Value = new System.DateTime(2029, 12, 31, 23, 59, 0, 0);
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(14, 57);
            this.label3.Margin = new System.Windows.Forms.Padding(5, 0, 5, 0);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(98, 32);
            this.label3.TabIndex = 15;
            this.label3.Text = "Date to:";
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(481, 17);
            this.label4.Margin = new System.Windows.Forms.Padding(5, 0, 5, 0);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(80, 32);
            this.label4.TabIndex = 16;
            this.label4.Text = "PESEL:";
            // 
            // inputPesel
            // 
            this.inputPesel.Location = new System.Drawing.Point(605, 14);
            this.inputPesel.Margin = new System.Windows.Forms.Padding(5, 6, 5, 6);
            this.inputPesel.Name = "inputPesel";
            this.inputPesel.Size = new System.Drawing.Size(349, 39);
            this.inputPesel.TabIndex = 17;
            // 
            // buttonClear
            // 
            this.buttonClear.Location = new System.Drawing.Point(964, 14);
            this.buttonClear.Margin = new System.Windows.Forms.Padding(5);
            this.buttonClear.Name = "buttonClear";
            this.buttonClear.Size = new System.Drawing.Size(377, 39);
            this.buttonClear.TabIndex = 18;
            this.buttonClear.Text = "Clear";
            this.buttonClear.UseVisualStyleBackColor = true;
            // 
            // VisitsForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(13F, 32F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1355, 691);
            this.Controls.Add(this.buttonClear);
            this.Controls.Add(this.inputPesel);
            this.Controls.Add(this.label4);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.inputDateTo);
            this.Controls.Add(this.inputDateFrom);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.inputLastName);
            this.Controls.Add(this.buttonSearch);
            this.Controls.Add(this.buttonSave);
            this.Controls.Add(this.dataVisits);
            this.Margin = new System.Windows.Forms.Padding(5);
            this.Name = "VisitsForm";
            this.Text = "Appointments";
            this.Load += new System.EventHandler(this.CancelVisitsForm_Load);
            ((System.ComponentModel.ISupportInitialize)(this.dataVisits)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.DataGridView dataVisits;
        private System.Windows.Forms.Button buttonSave;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.TextBox inputLastName;
        private System.Windows.Forms.Button buttonSearch;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.DateTimePicker inputDateFrom;
        private System.Windows.Forms.DateTimePicker inputDateTo;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.TextBox inputPesel;
        private System.Windows.Forms.Button buttonClear;
    }
}
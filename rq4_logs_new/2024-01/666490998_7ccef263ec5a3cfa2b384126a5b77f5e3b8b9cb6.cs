namespace LibraryApp.View
{
    partial class ListOfEmployeesForm
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(ListOfEmployeesForm));
            listOfEmployeesFormName = new Label();
            listOfEmployeesCloseLabel = new Label();
            employeesTable = new DataGridView();
            ((System.ComponentModel.ISupportInitialize)employeesTable).BeginInit();
            SuspendLayout();
            // 
            // listOfEmployeesFormName
            // 
            listOfEmployeesFormName.AutoSize = true;
            listOfEmployeesFormName.BackColor = Color.Transparent;
            listOfEmployeesFormName.Font = new Font("Segoe UI Semibold", 14.25F, FontStyle.Bold, GraphicsUnit.Point);
            listOfEmployeesFormName.ForeColor = Color.Black;
            listOfEmployeesFormName.Location = new Point(39, 32);
            listOfEmployeesFormName.Name = "listOfEmployeesFormName";
            listOfEmployeesFormName.Size = new Size(194, 25);
            listOfEmployeesFormName.TabIndex = 0;
            listOfEmployeesFormName.Text = "Список сотрудников";
            // 
            // listOfEmployeesCloseLabel
            // 
            listOfEmployeesCloseLabel.AutoSize = true;
            listOfEmployeesCloseLabel.Font = new Font("Lucida Console", 13.8F, FontStyle.Bold, GraphicsUnit.Point);
            listOfEmployeesCloseLabel.ForeColor = Color.Black;
            listOfEmployeesCloseLabel.Location = new Point(761, 15);
            listOfEmployeesCloseLabel.Name = "listOfEmployeesCloseLabel";
            listOfEmployeesCloseLabel.Size = new Size(21, 19);
            listOfEmployeesCloseLabel.TabIndex = 1;
            listOfEmployeesCloseLabel.Text = "-";
            listOfEmployeesCloseLabel.Click += ListOfEmployeesCloseLabel_Click;
            listOfEmployeesCloseLabel.MouseEnter += ListOfEmployeesCloseLabel_MouseEnter;
            listOfEmployeesCloseLabel.MouseLeave += ListOfEmployeesCloseLabel_MouseLeave;
            // 
            // employeesTable
            // 
            employeesTable.AllowUserToAddRows = false;
            employeesTable.AllowUserToResizeRows = false;
            employeesTable.AutoSizeColumnsMode = DataGridViewAutoSizeColumnsMode.Fill;
            employeesTable.BackgroundColor = SystemColors.Control;
            employeesTable.AlternatingRowsDefaultCellStyle.BackColor = Color.WhiteSmoke;
            employeesTable.BorderStyle = BorderStyle.None;
            employeesTable.ColumnHeadersHeightSizeMode = DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            employeesTable.Location = new Point(47, 69);
            employeesTable.Name = "employeesTable";
            employeesTable.RowHeadersVisible = false;
            employeesTable.RowTemplate.Height = 25;
            employeesTable.SelectionMode = DataGridViewSelectionMode.FullRowSelect;
            employeesTable.Size = new Size(712, 444);
            employeesTable.TabIndex = 2;
            // 
            // ListOfEmployeesForm
            // 
            AutoScaleDimensions = new SizeF(7F, 15F);
            AutoScaleMode = AutoScaleMode.Font;
            ClientSize = new Size(800, 550);
            Controls.Add(employeesTable);
            Controls.Add(listOfEmployeesCloseLabel);
            Controls.Add(listOfEmployeesFormName);
            FormBorderStyle = FormBorderStyle.None;
            Icon = (Icon)resources.GetObject("$this.Icon");
            Name = "ListOfEmployeesForm";
            StartPosition = FormStartPosition.CenterScreen;
            Text = "Список сотрудников";
            MouseDown += ThisForm_MouseDown;
            MouseMove += ThisForm_MouseMove;
            ((System.ComponentModel.ISupportInitialize)employeesTable).EndInit();
            ResumeLayout(false);
            PerformLayout();
        }

        #endregion

        private Label listOfEmployeesFormName;
        private Label listOfEmployeesCloseLabel;
        private DataGridView employeesTable;
    }
}
namespace Simsprojekat.View.StationManagerView
{
    partial class DevicesForm
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
            this.devicesGridView = new System.Windows.Forms.DataGridView();
            this.name = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.status = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.fixButton = new System.Windows.Forms.Button();
            this.messageLabel = new System.Windows.Forms.Label();
            ((System.ComponentModel.ISupportInitialize)(this.devicesGridView)).BeginInit();
            this.SuspendLayout();
            // 
            // devicesGridView
            // 
            this.devicesGridView.AllowUserToAddRows = false;
            this.devicesGridView.AllowUserToDeleteRows = false;
            this.devicesGridView.AllowUserToResizeColumns = false;
            this.devicesGridView.AllowUserToResizeRows = false;
            this.devicesGridView.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.devicesGridView.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.name,
            this.status});
            this.devicesGridView.Location = new System.Drawing.Point(54, 71);
            this.devicesGridView.Name = "devicesGridView";
            this.devicesGridView.RowTemplate.Height = 25;
            this.devicesGridView.Size = new System.Drawing.Size(243, 202);
            this.devicesGridView.TabIndex = 0;
            // 
            // name
            // 
            this.name.HeaderText = "Name";
            this.name.Name = "name";
            // 
            // status
            // 
            this.status.HeaderText = "Status";
            this.status.Name = "status";
            // 
            // fixButton
            // 
            this.fixButton.Location = new System.Drawing.Point(137, 314);
            this.fixButton.Name = "fixButton";
            this.fixButton.Size = new System.Drawing.Size(75, 23);
            this.fixButton.TabIndex = 1;
            this.fixButton.Text = "Fix";
            this.fixButton.UseVisualStyleBackColor = true;
            this.fixButton.Click += new System.EventHandler(this.fixButton_Click);
            // 
            // messageLabel
            // 
            this.messageLabel.AutoSize = true;
            this.messageLabel.Location = new System.Drawing.Point(103, 296);
            this.messageLabel.Name = "messageLabel";
            this.messageLabel.Size = new System.Drawing.Size(146, 15);
            this.messageLabel.TabIndex = 2;
            this.messageLabel.Text = "Device is working properly";
            this.messageLabel.Visible = false;
            // 
            // DevicesForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(7F, 15F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(351, 406);
            this.Controls.Add(this.messageLabel);
            this.Controls.Add(this.fixButton);
            this.Controls.Add(this.devicesGridView);
            this.Name = "DevicesForm";
            this.Text = "DevicesForm";
            this.Load += new System.EventHandler(this.DevicesForm_Load);
            ((System.ComponentModel.ISupportInitialize)(this.devicesGridView)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.DataGridView devicesGridView;
        private System.Windows.Forms.DataGridViewTextBoxColumn name;
        private System.Windows.Forms.DataGridViewTextBoxColumn status;
        private System.Windows.Forms.Button fixButton;
        private System.Windows.Forms.Label messageLabel;
    }
}
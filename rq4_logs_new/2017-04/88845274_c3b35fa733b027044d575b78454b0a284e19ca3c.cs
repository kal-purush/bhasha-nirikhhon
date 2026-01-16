namespace Ezzen
{
    partial class CreateGroupForm
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(CreateGroupForm));
            this.Create = new System.Windows.Forms.Button();
            this.Cancel = new System.Windows.Forms.Button();
            this.GNLabel = new System.Windows.Forms.Label();
            this.GroupName = new System.Windows.Forms.TextBox();
            this.SuspendLayout();
            // 
            // Create
            // 
            this.Create.BackColor = System.Drawing.Color.PaleGreen;
            this.Create.FlatAppearance.BorderSize = 0;
            this.Create.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.Create.Font = new System.Drawing.Font("Tw Cen MT", 12F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Create.Location = new System.Drawing.Point(168, 150);
            this.Create.Name = "Create";
            this.Create.Size = new System.Drawing.Size(80, 30);
            this.Create.TabIndex = 0;
            this.Create.Text = "Create";
            this.Create.UseVisualStyleBackColor = false;
            this.Create.Click += new System.EventHandler(this.Create_Click);
            // 
            // Cancel
            // 
            this.Cancel.FlatAppearance.BorderSize = 0;
            this.Cancel.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.Cancel.Font = new System.Drawing.Font("Tw Cen MT", 12F, System.Drawing.FontStyle.Bold);
            this.Cancel.Location = new System.Drawing.Point(252, 150);
            this.Cancel.Name = "Cancel";
            this.Cancel.Size = new System.Drawing.Size(84, 30);
            this.Cancel.TabIndex = 1;
            this.Cancel.Text = "Cancel";
            this.Cancel.UseVisualStyleBackColor = true;
            this.Cancel.Click += new System.EventHandler(this.Cancel_Click);
            // 
            // GNLabel
            // 
            this.GNLabel.AutoSize = true;
            this.GNLabel.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.GNLabel.Font = new System.Drawing.Font("Tw Cen MT", 12F, System.Drawing.FontStyle.Bold);
            this.GNLabel.Location = new System.Drawing.Point(35, 67);
            this.GNLabel.Name = "GNLabel";
            this.GNLabel.Size = new System.Drawing.Size(124, 23);
            this.GNLabel.TabIndex = 2;
            this.GNLabel.Text = "Group Name:";
            // 
            // GroupName
            // 
            this.GroupName.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.GroupName.Font = new System.Drawing.Font("Tw Cen MT", 12F, System.Drawing.FontStyle.Bold);
            this.GroupName.Location = new System.Drawing.Point(165, 68);
            this.GroupName.Name = "GroupName";
            this.GroupName.Size = new System.Drawing.Size(250, 22);
            this.GroupName.TabIndex = 3;
            // 
            // CreateGroupForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(500, 200);
            this.Controls.Add(this.GroupName);
            this.Controls.Add(this.GNLabel);
            this.Controls.Add(this.Cancel);
            this.Controls.Add(this.Create);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.None;
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.Name = "CreateGroupForm";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "CreateGroupForm";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button Create;
        private System.Windows.Forms.Button Cancel;
        private System.Windows.Forms.Label GNLabel;
        private System.Windows.Forms.TextBox GroupName;
    }
}
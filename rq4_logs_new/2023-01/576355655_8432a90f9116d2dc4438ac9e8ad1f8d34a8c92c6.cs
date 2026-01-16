using System.ComponentModel;

namespace Boxinator_V2.Usercontrol {
    partial class saveProject {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing) {
            if (disposing && (components != null)) {
                components.Dispose();
            }

            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent() {
            this.btnCancel = new System.Windows.Forms.Button();
            this.panel2 = new System.Windows.Forms.Panel();
            this.button1 = new System.Windows.Forms.Button();
            this.btnRaw = new System.Windows.Forms.Button();
            this.btnBoxinator = new System.Windows.Forms.Button();
            this.panel1 = new System.Windows.Forms.Panel();
            this.openProjectLabel = new System.Windows.Forms.Label();
            this.panel2.SuspendLayout();
            this.panel1.SuspendLayout();
            this.SuspendLayout();
            // 
            // btnCancel
            // 
            this.btnCancel.BackColor = System.Drawing.Color.FromArgb(((int) (((byte) (127)))), ((int) (((byte) (92)))), ((int) (((byte) (255)))));
            this.btnCancel.Cursor = System.Windows.Forms.Cursors.Hand;
            this.btnCancel.FlatAppearance.BorderSize = 0;
            this.btnCancel.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.btnCancel.Font = new System.Drawing.Font("Microsoft Sans Serif", 11.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte) (0)));
            this.btnCancel.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.btnCancel.Location = new System.Drawing.Point(67, 260);
            this.btnCancel.Name = "btnCancel";
            this.btnCancel.Size = new System.Drawing.Size(134, 44);
            this.btnCancel.TabIndex = 17;
            this.btnCancel.Text = "Cancel";
            this.btnCancel.UseVisualStyleBackColor = false;
            this.btnCancel.Click += new System.EventHandler(this.btnCancel_Click);
            // 
            // panel2
            // 
            this.panel2.Anchor = ((System.Windows.Forms.AnchorStyles) (((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) | System.Windows.Forms.AnchorStyles.Right)));
            this.panel2.BackColor = System.Drawing.Color.FromArgb(((int) (((byte) (12)))), ((int) (((byte) (22)))), ((int) (((byte) (31)))));
            this.panel2.Controls.Add(this.button1);
            this.panel2.Controls.Add(this.btnRaw);
            this.panel2.Controls.Add(this.btnBoxinator);
            this.panel2.Location = new System.Drawing.Point(17, 73);
            this.panel2.Name = "panel2";
            this.panel2.Padding = new System.Windows.Forms.Padding(5);
            this.panel2.Size = new System.Drawing.Size(226, 162);
            this.panel2.TabIndex = 16;
            // 
            // button1
            // 
            this.button1.BackColor = System.Drawing.Color.FromArgb(((int) (((byte) (127)))), ((int) (((byte) (92)))), ((int) (((byte) (255)))));
            this.button1.Cursor = System.Windows.Forms.Cursors.Hand;
            this.button1.FlatAppearance.BorderSize = 0;
            this.button1.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.button1.Font = new System.Drawing.Font("Microsoft Sans Serif", 11.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte) (0)));
            this.button1.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.button1.Location = new System.Drawing.Point(8, 108);
            this.button1.Name = "button1";
            this.button1.Size = new System.Drawing.Size(208, 44);
            this.button1.TabIndex = 13;
            this.button1.Text = "Coco format";
            this.button1.UseVisualStyleBackColor = false;
            // 
            // btnRaw
            // 
            this.btnRaw.BackColor = System.Drawing.Color.FromArgb(((int) (((byte) (127)))), ((int) (((byte) (92)))), ((int) (((byte) (255)))));
            this.btnRaw.Cursor = System.Windows.Forms.Cursors.Hand;
            this.btnRaw.FlatAppearance.BorderSize = 0;
            this.btnRaw.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.btnRaw.Font = new System.Drawing.Font("Microsoft Sans Serif", 11.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte) (0)));
            this.btnRaw.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.btnRaw.Location = new System.Drawing.Point(8, 58);
            this.btnRaw.Name = "btnRaw";
            this.btnRaw.Size = new System.Drawing.Size(208, 44);
            this.btnRaw.TabIndex = 12;
            this.btnRaw.Text = "Raw text";
            this.btnRaw.UseVisualStyleBackColor = false;
            // 
            // btnBoxinator
            // 
            this.btnBoxinator.BackColor = System.Drawing.Color.FromArgb(((int) (((byte) (127)))), ((int) (((byte) (92)))), ((int) (((byte) (255)))));
            this.btnBoxinator.Cursor = System.Windows.Forms.Cursors.Hand;
            this.btnBoxinator.FlatAppearance.BorderSize = 0;
            this.btnBoxinator.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.btnBoxinator.Font = new System.Drawing.Font("Microsoft Sans Serif", 11.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte) (0)));
            this.btnBoxinator.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.btnBoxinator.Location = new System.Drawing.Point(8, 8);
            this.btnBoxinator.Name = "btnBoxinator";
            this.btnBoxinator.Size = new System.Drawing.Size(208, 44);
            this.btnBoxinator.TabIndex = 11;
            this.btnBoxinator.Text = "Boxinator format";
            this.btnBoxinator.UseVisualStyleBackColor = false;
            // 
            // panel1
            // 
            this.panel1.BackColor = System.Drawing.Color.FromArgb(((int) (((byte) (7)))), ((int) (((byte) (14)))), ((int) (((byte) (21)))));
            this.panel1.Controls.Add(this.openProjectLabel);
            this.panel1.Dock = System.Windows.Forms.DockStyle.Top;
            this.panel1.Location = new System.Drawing.Point(0, 0);
            this.panel1.Name = "panel1";
            this.panel1.Size = new System.Drawing.Size(258, 57);
            this.panel1.TabIndex = 15;
            // 
            // openProjectLabel
            // 
            this.openProjectLabel.AutoSize = true;
            this.openProjectLabel.Font = new System.Drawing.Font("Microsoft Sans Serif", 11.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte) (0)));
            this.openProjectLabel.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.openProjectLabel.Location = new System.Drawing.Point(17, 19);
            this.openProjectLabel.Name = "openProjectLabel";
            this.openProjectLabel.Size = new System.Drawing.Size(125, 18);
            this.openProjectLabel.TabIndex = 0;
            this.openProjectLabel.Text = "Saving project..";
            // 
            // saveProjectt
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(10F, 18F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.Color.FromArgb(((int) (((byte) (7)))), ((int) (((byte) (14)))), ((int) (((byte) (21)))));
            this.ClientSize = new System.Drawing.Size(258, 318);
            this.Controls.Add(this.btnCancel);
            this.Controls.Add(this.panel2);
            this.Controls.Add(this.panel1);
            this.Font = new System.Drawing.Font("Microsoft Sans Serif", 11.25F, System.Drawing.FontStyle.Bold);
            this.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.None;
            this.Margin = new System.Windows.Forms.Padding(5, 4, 5, 4);
            this.Name = "saveProjectt";
            this.Text = "saveProjectt";
            this.panel2.ResumeLayout(false);
            this.panel1.ResumeLayout(false);
            this.panel1.PerformLayout();
            this.ResumeLayout(false);
        }

        private System.Windows.Forms.Button btnCancel;
        private System.Windows.Forms.Panel panel2;
        private System.Windows.Forms.Button button1;
        private System.Windows.Forms.Button btnRaw;
        private System.Windows.Forms.Button btnBoxinator;
        private System.Windows.Forms.Panel panel1;
        private System.Windows.Forms.Label openProjectLabel;

        #endregion
    }
}
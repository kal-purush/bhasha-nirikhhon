namespace HostelManagmentProject
{
    partial class hostelDetail
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
            this.txthostelname = new System.Windows.Forms.TextBox();
            this.txtnuofrooms = new System.Windows.Forms.TextBox();
            this.gvrtlist = new System.Windows.Forms.DataGridView();
            this.gvgatekeeperlist = new System.Windows.Forms.DataGridView();
            this.linkLabel1 = new System.Windows.Forms.LinkLabel();
            this.label2 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.menuStrip2 = new System.Windows.Forms.MenuStrip();
            this.toolStripMenuItem2 = new System.Windows.Forms.ToolStripMenuItem();
            this.label1 = new System.Windows.Forms.Label();
            this.label4 = new System.Windows.Forms.Label();
            ((System.ComponentModel.ISupportInitialize)(this.gvrtlist)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.gvgatekeeperlist)).BeginInit();
            this.menuStrip2.SuspendLayout();
            this.SuspendLayout();
            // 
            // txthostelname
            // 
            this.txthostelname.Location = new System.Drawing.Point(279, 84);
            this.txthostelname.Name = "txthostelname";
            this.txthostelname.Size = new System.Drawing.Size(100, 20);
            this.txthostelname.TabIndex = 0;
            // 
            // txtnuofrooms
            // 
            this.txtnuofrooms.Location = new System.Drawing.Point(751, 84);
            this.txtnuofrooms.Name = "txtnuofrooms";
            this.txtnuofrooms.Size = new System.Drawing.Size(100, 20);
            this.txtnuofrooms.TabIndex = 1;
            // 
            // gvrtlist
            // 
            this.gvrtlist.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.gvrtlist.Location = new System.Drawing.Point(62, 171);
            this.gvrtlist.Name = "gvrtlist";
            this.gvrtlist.Size = new System.Drawing.Size(317, 150);
            this.gvrtlist.TabIndex = 2;
            // 
            // gvgatekeeperlist
            // 
            this.gvgatekeeperlist.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.gvgatekeeperlist.Location = new System.Drawing.Point(534, 171);
            this.gvgatekeeperlist.Name = "gvgatekeeperlist";
            this.gvgatekeeperlist.Size = new System.Drawing.Size(317, 150);
            this.gvgatekeeperlist.TabIndex = 3;
            // 
            // linkLabel1
            // 
            this.linkLabel1.AutoSize = true;
            this.linkLabel1.Location = new System.Drawing.Point(34, 434);
            this.linkLabel1.Name = "linkLabel1";
            this.linkLabel1.Size = new System.Drawing.Size(104, 13);
            this.linkLabel1.TabIndex = 6;
            this.linkLabel1.TabStop = true;
            this.linkLabel1.Text = "Go to Previous page";
            this.linkLabel1.LinkClicked += new System.Windows.Forms.LinkLabelLinkClickedEventHandler(this.linkLabel1_LinkClicked);
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(59, 87);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(66, 13);
            this.label2.TabIndex = 7;
            this.label2.Text = "Hostel name";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(531, 87);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(87, 13);
            this.label3.TabIndex = 8;
            this.label3.Text = "Number of rooms";
            // 
            // menuStrip2
            // 
            this.menuStrip2.BackColor = System.Drawing.Color.SteelBlue;
            this.menuStrip2.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.toolStripMenuItem2});
            this.menuStrip2.Location = new System.Drawing.Point(0, 0);
            this.menuStrip2.Name = "menuStrip2";
            this.menuStrip2.Size = new System.Drawing.Size(1038, 24);
            this.menuStrip2.TabIndex = 197;
            this.menuStrip2.Text = "menuStrip2";
            // 
            // toolStripMenuItem2
            // 
            this.toolStripMenuItem2.BackColor = System.Drawing.Color.SteelBlue;
            this.toolStripMenuItem2.ForeColor = System.Drawing.SystemColors.ControlLightLight;
            this.toolStripMenuItem2.Name = "toolStripMenuItem2";
            this.toolStripMenuItem2.Size = new System.Drawing.Size(55, 20);
            this.toolStripMenuItem2.Text = "Admin";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(173, 146);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(37, 13);
            this.label1.TabIndex = 198;
            this.label1.Text = "Rt List";
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(651, 146);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(81, 13);
            this.label4.TabIndex = 199;
            this.label4.Text = "Gate keeper list";
            // 
            // hostelDetail
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.Color.White;
            this.ClientSize = new System.Drawing.Size(1038, 516);
            this.Controls.Add(this.label4);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.menuStrip2);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.linkLabel1);
            this.Controls.Add(this.gvgatekeeperlist);
            this.Controls.Add(this.gvrtlist);
            this.Controls.Add(this.txtnuofrooms);
            this.Controls.Add(this.txthostelname);
            this.Name = "hostelDetail";
            this.Text = "hostelDetail";
            this.Load += new System.EventHandler(this.hostelDetail_Load);
            ((System.ComponentModel.ISupportInitialize)(this.gvrtlist)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.gvgatekeeperlist)).EndInit();
            this.menuStrip2.ResumeLayout(false);
            this.menuStrip2.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox txthostelname;
        private System.Windows.Forms.TextBox txtnuofrooms;
        private System.Windows.Forms.DataGridView gvrtlist;
        private System.Windows.Forms.DataGridView gvgatekeeperlist;
        private System.Windows.Forms.LinkLabel linkLabel1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.MenuStrip menuStrip2;
        private System.Windows.Forms.ToolStripMenuItem toolStripMenuItem2;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label4;
    }
}
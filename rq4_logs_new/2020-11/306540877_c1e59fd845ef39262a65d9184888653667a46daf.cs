namespace FMSAPP
{
    partial class MDIParent1
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(MDIParent1));
            this.menuStrip = new System.Windows.Forms.MenuStrip();
            this.adminToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.animeFormToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.userFormToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.adminFormToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.closeAllFormToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolStripSeparator1 = new System.Windows.Forms.ToolStripSeparator();
            this.logoutToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.showTotalToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.totalAnimeToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.totalUserToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.totalAdminToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.totalGenreToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.totalSeasonToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.totalStudioToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.hideAllToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.toolTip = new System.Windows.Forms.ToolTip(this.components);
            this.lblID = new System.Windows.Forms.Label();
            this.panelTotalAnime = new System.Windows.Forms.Panel();
            this.lblAnime = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.panelTotalGenre = new System.Windows.Forms.Panel();
            this.lblGenre = new System.Windows.Forms.Label();
            this.label4 = new System.Windows.Forms.Label();
            this.panelTotalUser = new System.Windows.Forms.Panel();
            this.lblUser = new System.Windows.Forms.Label();
            this.label6 = new System.Windows.Forms.Label();
            this.panelTotalSeason = new System.Windows.Forms.Panel();
            this.lblSeason = new System.Windows.Forms.Label();
            this.label8 = new System.Windows.Forms.Label();
            this.panelTotalAdmin = new System.Windows.Forms.Panel();
            this.lblAdmin = new System.Windows.Forms.Label();
            this.label10 = new System.Windows.Forms.Label();
            this.panelTotalStudio = new System.Windows.Forms.Panel();
            this.lblStudio = new System.Windows.Forms.Label();
            this.label12 = new System.Windows.Forms.Label();
            this.menuStrip.SuspendLayout();
            this.panelTotalAnime.SuspendLayout();
            this.panelTotalGenre.SuspendLayout();
            this.panelTotalUser.SuspendLayout();
            this.panelTotalSeason.SuspendLayout();
            this.panelTotalAdmin.SuspendLayout();
            this.panelTotalStudio.SuspendLayout();
            this.SuspendLayout();
            // 
            // menuStrip
            // 
            this.menuStrip.ImageScalingSize = new System.Drawing.Size(20, 20);
            this.menuStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.adminToolStripMenuItem,
            this.showTotalToolStripMenuItem});
            this.menuStrip.Location = new System.Drawing.Point(0, 0);
            this.menuStrip.Name = "menuStrip";
            this.menuStrip.Size = new System.Drawing.Size(930, 28);
            this.menuStrip.TabIndex = 0;
            this.menuStrip.Text = "MenuStrip";
            // 
            // adminToolStripMenuItem
            // 
            this.adminToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.animeFormToolStripMenuItem,
            this.userFormToolStripMenuItem,
            this.adminFormToolStripMenuItem,
            this.closeAllFormToolStripMenuItem,
            this.toolStripSeparator1,
            this.logoutToolStripMenuItem});
            this.adminToolStripMenuItem.Name = "adminToolStripMenuItem";
            this.adminToolStripMenuItem.Size = new System.Drawing.Size(67, 24);
            this.adminToolStripMenuItem.Text = "Admin";
            // 
            // animeFormToolStripMenuItem
            // 
            this.animeFormToolStripMenuItem.BackColor = System.Drawing.Color.DarkOrchid;
            this.animeFormToolStripMenuItem.Name = "animeFormToolStripMenuItem";
            this.animeFormToolStripMenuItem.Size = new System.Drawing.Size(224, 26);
            this.animeFormToolStripMenuItem.Text = "AnimeForm";
            this.animeFormToolStripMenuItem.Click += new System.EventHandler(this.animeFormToolStripMenuItem_Click);
            // 
            // userFormToolStripMenuItem
            // 
            this.userFormToolStripMenuItem.BackColor = System.Drawing.Color.Aqua;
            this.userFormToolStripMenuItem.Name = "userFormToolStripMenuItem";
            this.userFormToolStripMenuItem.Size = new System.Drawing.Size(224, 26);
            this.userFormToolStripMenuItem.Text = "UserForm";
            this.userFormToolStripMenuItem.Click += new System.EventHandler(this.userFormToolStripMenuItem_Click);
            // 
            // adminFormToolStripMenuItem
            // 
            this.adminFormToolStripMenuItem.BackColor = System.Drawing.SystemColors.Highlight;
            this.adminFormToolStripMenuItem.Name = "adminFormToolStripMenuItem";
            this.adminFormToolStripMenuItem.Size = new System.Drawing.Size(224, 26);
            this.adminFormToolStripMenuItem.Text = "AdminForm";
            this.adminFormToolStripMenuItem.Click += new System.EventHandler(this.adminFormToolStripMenuItem_Click);
            // 
            // closeAllFormToolStripMenuItem
            // 
            this.closeAllFormToolStripMenuItem.BackColor = System.Drawing.SystemColors.ControlLightLight;
            this.closeAllFormToolStripMenuItem.Name = "closeAllFormToolStripMenuItem";
            this.closeAllFormToolStripMenuItem.Size = new System.Drawing.Size(224, 26);
            this.closeAllFormToolStripMenuItem.Text = "CloseAllForm";
            this.closeAllFormToolStripMenuItem.Click += new System.EventHandler(this.closeAllFormToolStripMenuItem_Click);
            // 
            // toolStripSeparator1
            // 
            this.toolStripSeparator1.Name = "toolStripSeparator1";
            this.toolStripSeparator1.Size = new System.Drawing.Size(221, 6);
            // 
            // logoutToolStripMenuItem
            // 
            this.logoutToolStripMenuItem.BackColor = System.Drawing.Color.Turquoise;
            this.logoutToolStripMenuItem.Name = "logoutToolStripMenuItem";
            this.logoutToolStripMenuItem.Size = new System.Drawing.Size(224, 26);
            this.logoutToolStripMenuItem.Text = "LogOut";
            this.logoutToolStripMenuItem.Click += new System.EventHandler(this.logoutToolStripMenuItem_Click);
            // 
            // showTotalToolStripMenuItem
            // 
            this.showTotalToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.totalAnimeToolStripMenuItem,
            this.totalUserToolStripMenuItem,
            this.totalAdminToolStripMenuItem,
            this.totalGenreToolStripMenuItem,
            this.totalSeasonToolStripMenuItem,
            this.totalStudioToolStripMenuItem,
            this.hideAllToolStripMenuItem});
            this.showTotalToolStripMenuItem.Name = "showTotalToolStripMenuItem";
            this.showTotalToolStripMenuItem.Size = new System.Drawing.Size(92, 24);
            this.showTotalToolStripMenuItem.Text = "ShowTotal";
            this.showTotalToolStripMenuItem.Click += new System.EventHandler(this.showTotalToolStripMenuItem_Click);
            // 
            // totalAnimeToolStripMenuItem
            // 
            this.totalAnimeToolStripMenuItem.Name = "totalAnimeToolStripMenuItem";
            this.totalAnimeToolStripMenuItem.Size = new System.Drawing.Size(172, 26);
            this.totalAnimeToolStripMenuItem.Text = "Total Anime";
            this.totalAnimeToolStripMenuItem.Click += new System.EventHandler(this.totalAnimeToolStripMenuItem_Click);
            // 
            // totalUserToolStripMenuItem
            // 
            this.totalUserToolStripMenuItem.Name = "totalUserToolStripMenuItem";
            this.totalUserToolStripMenuItem.Size = new System.Drawing.Size(172, 26);
            this.totalUserToolStripMenuItem.Text = "TotalUser";
            this.totalUserToolStripMenuItem.Click += new System.EventHandler(this.totalUserToolStripMenuItem_Click);
            // 
            // totalAdminToolStripMenuItem
            // 
            this.totalAdminToolStripMenuItem.Name = "totalAdminToolStripMenuItem";
            this.totalAdminToolStripMenuItem.Size = new System.Drawing.Size(172, 26);
            this.totalAdminToolStripMenuItem.Text = "TotalAdmin";
            this.totalAdminToolStripMenuItem.Click += new System.EventHandler(this.totalAdminToolStripMenuItem_Click);
            // 
            // totalGenreToolStripMenuItem
            // 
            this.totalGenreToolStripMenuItem.Name = "totalGenreToolStripMenuItem";
            this.totalGenreToolStripMenuItem.Size = new System.Drawing.Size(172, 26);
            this.totalGenreToolStripMenuItem.Text = "TotalGenre";
            this.totalGenreToolStripMenuItem.Click += new System.EventHandler(this.totalGenreToolStripMenuItem_Click);
            // 
            // totalSeasonToolStripMenuItem
            // 
            this.totalSeasonToolStripMenuItem.Name = "totalSeasonToolStripMenuItem";
            this.totalSeasonToolStripMenuItem.Size = new System.Drawing.Size(172, 26);
            this.totalSeasonToolStripMenuItem.Text = "TotalSeason";
            this.totalSeasonToolStripMenuItem.Click += new System.EventHandler(this.totalSeasonToolStripMenuItem_Click);
            // 
            // totalStudioToolStripMenuItem
            // 
            this.totalStudioToolStripMenuItem.Name = "totalStudioToolStripMenuItem";
            this.totalStudioToolStripMenuItem.Size = new System.Drawing.Size(172, 26);
            this.totalStudioToolStripMenuItem.Text = "TotalStudio";
            this.totalStudioToolStripMenuItem.Click += new System.EventHandler(this.totalStudioToolStripMenuItem_Click);
            // 
            // hideAllToolStripMenuItem
            // 
            this.hideAllToolStripMenuItem.Name = "hideAllToolStripMenuItem";
            this.hideAllToolStripMenuItem.Size = new System.Drawing.Size(172, 26);
            this.hideAllToolStripMenuItem.Text = "HideAll";
            this.hideAllToolStripMenuItem.Click += new System.EventHandler(this.hideAllToolStripMenuItem_Click);
            // 
            // lblID
            // 
            this.lblID.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.lblID.AutoSize = true;
            this.lblID.Font = new System.Drawing.Font("Microsoft Sans Serif", 10.8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblID.Location = new System.Drawing.Point(366, 84);
            this.lblID.Name = "lblID";
            this.lblID.Size = new System.Drawing.Size(129, 24);
            this.lblID.TabIndex = 4;
            this.lblID.Text = "IDAdminLogin";
            this.lblID.Visible = false;
            // 
            // panelTotalAnime
            // 
            this.panelTotalAnime.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.panelTotalAnime.Controls.Add(this.lblAnime);
            this.panelTotalAnime.Controls.Add(this.label1);
            this.panelTotalAnime.Location = new System.Drawing.Point(12, 43);
            this.panelTotalAnime.Name = "panelTotalAnime";
            this.panelTotalAnime.Size = new System.Drawing.Size(290, 100);
            this.panelTotalAnime.TabIndex = 6;
            this.panelTotalAnime.Visible = false;
            // 
            // lblAnime
            // 
            this.lblAnime.AutoSize = true;
            this.lblAnime.Font = new System.Drawing.Font("Microsoft Sans Serif", 10.2F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblAnime.Location = new System.Drawing.Point(176, 41);
            this.lblAnime.Name = "lblAnime";
            this.lblAnime.Size = new System.Drawing.Size(59, 20);
            this.lblAnime.TabIndex = 8;
            this.lblAnime.Text = "label2";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Tempus Sans ITC", 10.2F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.ForeColor = System.Drawing.SystemColors.Highlight;
            this.label1.Location = new System.Drawing.Point(36, 41);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(110, 22);
            this.label1.TabIndex = 7;
            this.label1.Text = "Total Animes:";
            // 
            // panelTotalGenre
            // 
            this.panelTotalGenre.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.panelTotalGenre.Controls.Add(this.lblGenre);
            this.panelTotalGenre.Controls.Add(this.label4);
            this.panelTotalGenre.Location = new System.Drawing.Point(582, 43);
            this.panelTotalGenre.Name = "panelTotalGenre";
            this.panelTotalGenre.Size = new System.Drawing.Size(290, 100);
            this.panelTotalGenre.TabIndex = 9;
            this.panelTotalGenre.Visible = false;
            // 
            // lblGenre
            // 
            this.lblGenre.AutoSize = true;
            this.lblGenre.Font = new System.Drawing.Font("Microsoft Sans Serif", 10.2F, System.Drawing.FontStyle.Bold);
            this.lblGenre.Location = new System.Drawing.Point(176, 41);
            this.lblGenre.Name = "lblGenre";
            this.lblGenre.Size = new System.Drawing.Size(59, 20);
            this.lblGenre.TabIndex = 8;
            this.lblGenre.Text = "label3";
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Font = new System.Drawing.Font("French Script MT", 16.2F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label4.ForeColor = System.Drawing.Color.Tomato;
            this.label4.Location = new System.Drawing.Point(35, 33);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(124, 32);
            this.label4.TabIndex = 7;
            this.label4.Text = "Total Genres:";
            // 
            // panelTotalUser
            // 
            this.panelTotalUser.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.panelTotalUser.Controls.Add(this.lblUser);
            this.panelTotalUser.Controls.Add(this.label6);
            this.panelTotalUser.Location = new System.Drawing.Point(12, 199);
            this.panelTotalUser.Name = "panelTotalUser";
            this.panelTotalUser.Size = new System.Drawing.Size(290, 100);
            this.panelTotalUser.TabIndex = 9;
            this.panelTotalUser.Visible = false;
            // 
            // lblUser
            // 
            this.lblUser.AutoSize = true;
            this.lblUser.Font = new System.Drawing.Font("Microsoft Sans Serif", 10.2F, System.Drawing.FontStyle.Bold);
            this.lblUser.Location = new System.Drawing.Point(176, 41);
            this.lblUser.Name = "lblUser";
            this.lblUser.Size = new System.Drawing.Size(59, 20);
            this.lblUser.TabIndex = 8;
            this.lblUser.Text = "label5";
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Font = new System.Drawing.Font("MingLiU_HKSCS-ExtB", 10.2F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label6.ForeColor = System.Drawing.Color.LimeGreen;
            this.label6.Location = new System.Drawing.Point(36, 41);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(116, 17);
            this.label6.TabIndex = 7;
            this.label6.Text = "Total Users:";
            // 
            // panelTotalSeason
            // 
            this.panelTotalSeason.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.panelTotalSeason.Controls.Add(this.lblSeason);
            this.panelTotalSeason.Controls.Add(this.label8);
            this.panelTotalSeason.Location = new System.Drawing.Point(582, 199);
            this.panelTotalSeason.Name = "panelTotalSeason";
            this.panelTotalSeason.Size = new System.Drawing.Size(290, 100);
            this.panelTotalSeason.TabIndex = 10;
            this.panelTotalSeason.Visible = false;
            // 
            // lblSeason
            // 
            this.lblSeason.AutoSize = true;
            this.lblSeason.Font = new System.Drawing.Font("Microsoft Sans Serif", 10.2F, System.Drawing.FontStyle.Bold);
            this.lblSeason.Location = new System.Drawing.Point(176, 41);
            this.lblSeason.Name = "lblSeason";
            this.lblSeason.Size = new System.Drawing.Size(59, 20);
            this.lblSeason.TabIndex = 8;
            this.lblSeason.Text = "label7";
            // 
            // label8
            // 
            this.label8.AutoSize = true;
            this.label8.Font = new System.Drawing.Font("Vivaldi", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label8.ForeColor = System.Drawing.Color.DarkGray;
            this.label8.Location = new System.Drawing.Point(36, 41);
            this.label8.Name = "label8";
            this.label8.Size = new System.Drawing.Size(121, 24);
            this.label8.TabIndex = 7;
            this.label8.Text = "Total Seasons:";
            // 
            // panelTotalAdmin
            // 
            this.panelTotalAdmin.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.panelTotalAdmin.Controls.Add(this.lblAdmin);
            this.panelTotalAdmin.Controls.Add(this.label10);
            this.panelTotalAdmin.Location = new System.Drawing.Point(12, 343);
            this.panelTotalAdmin.Name = "panelTotalAdmin";
            this.panelTotalAdmin.Size = new System.Drawing.Size(290, 100);
            this.panelTotalAdmin.TabIndex = 10;
            this.panelTotalAdmin.Visible = false;
            // 
            // lblAdmin
            // 
            this.lblAdmin.AutoSize = true;
            this.lblAdmin.Font = new System.Drawing.Font("Microsoft Sans Serif", 10.2F, System.Drawing.FontStyle.Bold);
            this.lblAdmin.Location = new System.Drawing.Point(176, 41);
            this.lblAdmin.Name = "lblAdmin";
            this.lblAdmin.Size = new System.Drawing.Size(59, 20);
            this.lblAdmin.TabIndex = 8;
            this.lblAdmin.Text = "label9";
            // 
            // label10
            // 
            this.label10.AutoSize = true;
            this.label10.Font = new System.Drawing.Font("Matura MT Script Capitals", 10.2F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label10.ForeColor = System.Drawing.Color.Gold;
            this.label10.Location = new System.Drawing.Point(24, 41);
            this.label10.Name = "label10";
            this.label10.Size = new System.Drawing.Size(128, 23);
            this.label10.TabIndex = 7;
            this.label10.Text = "Total Admins:";
            // 
            // panelTotalStudio
            // 
            this.panelTotalStudio.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.panelTotalStudio.Controls.Add(this.lblStudio);
            this.panelTotalStudio.Controls.Add(this.label12);
            this.panelTotalStudio.Location = new System.Drawing.Point(582, 343);
            this.panelTotalStudio.Name = "panelTotalStudio";
            this.panelTotalStudio.Size = new System.Drawing.Size(290, 100);
            this.panelTotalStudio.TabIndex = 11;
            this.panelTotalStudio.Visible = false;
            // 
            // lblStudio
            // 
            this.lblStudio.AutoSize = true;
            this.lblStudio.Font = new System.Drawing.Font("Microsoft Sans Serif", 10.2F, System.Drawing.FontStyle.Bold);
            this.lblStudio.Location = new System.Drawing.Point(176, 41);
            this.lblStudio.Name = "lblStudio";
            this.lblStudio.Size = new System.Drawing.Size(69, 20);
            this.lblStudio.TabIndex = 8;
            this.lblStudio.Text = "label11";
            // 
            // label12
            // 
            this.label12.AutoSize = true;
            this.label12.Font = new System.Drawing.Font("Rage Italic", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label12.ForeColor = System.Drawing.Color.Maroon;
            this.label12.Location = new System.Drawing.Point(36, 41);
            this.label12.Name = "label12";
            this.label12.Size = new System.Drawing.Size(115, 25);
            this.label12.TabIndex = 7;
            this.label12.Text = "Total Studios:";
            // 
            // MDIParent1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.AutoSize = true;
            this.BackColor = System.Drawing.Color.Lavender;
            this.BackgroundImage = ((System.Drawing.Image)(resources.GetObject("$this.BackgroundImage")));
            this.ClientSize = new System.Drawing.Size(930, 583);
            this.Controls.Add(this.panelTotalStudio);
            this.Controls.Add(this.panelTotalAdmin);
            this.Controls.Add(this.panelTotalSeason);
            this.Controls.Add(this.panelTotalUser);
            this.Controls.Add(this.panelTotalGenre);
            this.Controls.Add(this.panelTotalAnime);
            this.Controls.Add(this.lblID);
            this.Controls.Add(this.menuStrip);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedSingle;
            this.IsMdiContainer = true;
            this.MainMenuStrip = this.menuStrip;
            this.Margin = new System.Windows.Forms.Padding(4);
            this.Name = "MDIParent1";
            this.Text = "DashboardAdmin";
            this.Load += new System.EventHandler(this.MDIParent1_Load_1);
            this.menuStrip.ResumeLayout(false);
            this.menuStrip.PerformLayout();
            this.panelTotalAnime.ResumeLayout(false);
            this.panelTotalAnime.PerformLayout();
            this.panelTotalGenre.ResumeLayout(false);
            this.panelTotalGenre.PerformLayout();
            this.panelTotalUser.ResumeLayout(false);
            this.panelTotalUser.PerformLayout();
            this.panelTotalSeason.ResumeLayout(false);
            this.panelTotalSeason.PerformLayout();
            this.panelTotalAdmin.ResumeLayout(false);
            this.panelTotalAdmin.PerformLayout();
            this.panelTotalStudio.ResumeLayout(false);
            this.panelTotalStudio.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }
        #endregion


        private System.Windows.Forms.MenuStrip menuStrip;
        private System.Windows.Forms.ToolTip toolTip;
        private System.Windows.Forms.ToolStripMenuItem adminToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem animeFormToolStripMenuItem;
        private System.Windows.Forms.Label lblID;
        private System.Windows.Forms.ToolStripMenuItem userFormToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem closeAllFormToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem adminFormToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem logoutToolStripMenuItem;
        private System.Windows.Forms.Panel panelTotalAnime;
        private System.Windows.Forms.Label lblAnime;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Panel panelTotalGenre;
        private System.Windows.Forms.Label lblGenre;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.Panel panelTotalUser;
        private System.Windows.Forms.Label lblUser;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.Panel panelTotalSeason;
        private System.Windows.Forms.Label lblSeason;
        private System.Windows.Forms.Label label8;
        private System.Windows.Forms.Panel panelTotalAdmin;
        private System.Windows.Forms.Label lblAdmin;
        private System.Windows.Forms.Label label10;
        private System.Windows.Forms.Panel panelTotalStudio;
        private System.Windows.Forms.Label lblStudio;
        private System.Windows.Forms.Label label12;
        private System.Windows.Forms.ToolStripMenuItem showTotalToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem totalUserToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem totalAdminToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem totalGenreToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem totalSeasonToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem totalStudioToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem totalAnimeToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem hideAllToolStripMenuItem;
        private System.Windows.Forms.ToolStripSeparator toolStripSeparator1;
    }
}



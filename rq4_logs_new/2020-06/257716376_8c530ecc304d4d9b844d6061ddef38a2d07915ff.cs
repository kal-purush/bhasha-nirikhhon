namespace Interfaz_FyBuzz_MOD1
{
    partial class Form1
    {
        /// <summary>
        /// Variable del diseñador necesaria.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Limpiar los recursos que se estén usando.
        /// </summary>
        /// <param name="disposing">true si los recursos administrados se deben desechar; false en caso contrario.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Código generado por el Diseñador de Windows Forms

        /// <summary>
        /// Método necesario para admitir el Diseñador. No se puede modificar
        /// el contenido de este método con el editor de código.
        /// </summary>
        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(Form1));
            this.SideMenu = new System.Windows.Forms.Panel();
            this.AboutUsButton = new System.Windows.Forms.Button();
            this.CreateOptionsPanel = new System.Windows.Forms.Panel();
            this.CreateProfilesButton = new System.Windows.Forms.Button();
            this.CreatePlayListsButton = new System.Windows.Forms.Button();
            this.CreateVideosButton = new System.Windows.Forms.Button();
            this.CreateSongsButton = new System.Windows.Forms.Button();
            this.CreateButton = new System.Windows.Forms.Button();
            this.PlayListsOptionsPanel = new System.Windows.Forms.Panel();
            this.FavoritePlsButton = new System.Windows.Forms.Button();
            this.GlobalPlsButton = new System.Windows.Forms.Button();
            this.PrivatePlsButton = new System.Windows.Forms.Button();
            this.PlayListsButton = new System.Windows.Forms.Button();
            this.MultimediaIOptionsPanel = new System.Windows.Forms.Panel();
            this.VideosButton = new System.Windows.Forms.Button();
            this.SongsButton = new System.Windows.Forms.Button();
            this.MultimediaButton = new System.Windows.Forms.Button();
            this.LogoFyBuzz = new System.Windows.Forms.Panel();
            this.pictureBox1 = new System.Windows.Forms.PictureBox();
            this.PlayerPanel = new System.Windows.Forms.Panel();
            this.RateMediaButton = new FontAwesome.Sharp.IconButton();
            this.LikeMediaButton = new FontAwesome.Sharp.IconButton();
            this.InfoMediaButton = new FontAwesome.Sharp.IconButton();
            this.BackWardMediaButton = new FontAwesome.Sharp.IconButton();
            this.ForwardMediaButton = new FontAwesome.Sharp.IconButton();
            this.PlayMediaButton = new FontAwesome.Sharp.IconButton();
            this.PauseMediaButton = new FontAwesome.Sharp.IconButton();
            this.ProgressBarMedia = new System.Windows.Forms.ProgressBar();
            this.GeneralPanel = new System.Windows.Forms.Panel();
            this.pictureBox2 = new System.Windows.Forms.PictureBox();
            this.contextMenuStrip1 = new System.Windows.Forms.ContextMenuStrip(this.components);
            this.textBox1 = new System.Windows.Forms.TextBox();
            this.iconButton1 = new FontAwesome.Sharp.IconButton();
            this.textBox3 = new System.Windows.Forms.TextBox();
            this.SideMenu.SuspendLayout();
            this.CreateOptionsPanel.SuspendLayout();
            this.PlayListsOptionsPanel.SuspendLayout();
            this.MultimediaIOptionsPanel.SuspendLayout();
            this.LogoFyBuzz.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox1)).BeginInit();
            this.PlayerPanel.SuspendLayout();
            this.GeneralPanel.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox2)).BeginInit();
            this.SuspendLayout();
            // 
            // SideMenu
            // 
            this.SideMenu.AutoScroll = true;
            this.SideMenu.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(8)))), ((int)(((byte)(8)))), ((int)(((byte)(44)))));
            this.SideMenu.Controls.Add(this.AboutUsButton);
            this.SideMenu.Controls.Add(this.CreateOptionsPanel);
            this.SideMenu.Controls.Add(this.CreateButton);
            this.SideMenu.Controls.Add(this.PlayListsOptionsPanel);
            this.SideMenu.Controls.Add(this.PlayListsButton);
            this.SideMenu.Controls.Add(this.MultimediaIOptionsPanel);
            this.SideMenu.Controls.Add(this.MultimediaButton);
            this.SideMenu.Controls.Add(this.LogoFyBuzz);
            this.SideMenu.Dock = System.Windows.Forms.DockStyle.Left;
            this.SideMenu.Location = new System.Drawing.Point(0, 0);
            this.SideMenu.Name = "SideMenu";
            this.SideMenu.Size = new System.Drawing.Size(250, 561);
            this.SideMenu.TabIndex = 0;
            // 
            // AboutUsButton
            // 
            this.AboutUsButton.Dock = System.Windows.Forms.DockStyle.Top;
            this.AboutUsButton.FlatAppearance.BorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(234)))), ((int)(((byte)(188)))), ((int)(((byte)(45)))));
            this.AboutUsButton.FlatAppearance.BorderSize = 3;
            this.AboutUsButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.AboutUsButton.Font = new System.Drawing.Font("Century Gothic", 10F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.AboutUsButton.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.AboutUsButton.Location = new System.Drawing.Point(0, 611);
            this.AboutUsButton.Name = "AboutUsButton";
            this.AboutUsButton.Size = new System.Drawing.Size(233, 45);
            this.AboutUsButton.TabIndex = 7;
            this.AboutUsButton.Text = "About FyBuzz";
            this.AboutUsButton.UseVisualStyleBackColor = true;
            this.AboutUsButton.Click += new System.EventHandler(this.AboutUsButton_Click);
            // 
            // CreateOptionsPanel
            // 
            this.CreateOptionsPanel.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(4)))), ((int)(((byte)(20)))), ((int)(((byte)(34)))));
            this.CreateOptionsPanel.Controls.Add(this.CreateProfilesButton);
            this.CreateOptionsPanel.Controls.Add(this.CreatePlayListsButton);
            this.CreateOptionsPanel.Controls.Add(this.CreateVideosButton);
            this.CreateOptionsPanel.Controls.Add(this.CreateSongsButton);
            this.CreateOptionsPanel.Dock = System.Windows.Forms.DockStyle.Top;
            this.CreateOptionsPanel.Location = new System.Drawing.Point(0, 446);
            this.CreateOptionsPanel.Name = "CreateOptionsPanel";
            this.CreateOptionsPanel.Size = new System.Drawing.Size(233, 165);
            this.CreateOptionsPanel.TabIndex = 6;
            // 
            // CreateProfilesButton
            // 
            this.CreateProfilesButton.Dock = System.Windows.Forms.DockStyle.Top;
            this.CreateProfilesButton.FlatAppearance.BorderSize = 0;
            this.CreateProfilesButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.CreateProfilesButton.Font = new System.Drawing.Font("Century Gothic", 10F);
            this.CreateProfilesButton.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.CreateProfilesButton.Location = new System.Drawing.Point(0, 120);
            this.CreateProfilesButton.Name = "CreateProfilesButton";
            this.CreateProfilesButton.Size = new System.Drawing.Size(233, 40);
            this.CreateProfilesButton.TabIndex = 3;
            this.CreateProfilesButton.Text = "Profiles";
            this.CreateProfilesButton.UseVisualStyleBackColor = true;
            this.CreateProfilesButton.Click += new System.EventHandler(this.CreateProfilesButton_Click);
            // 
            // CreatePlayListsButton
            // 
            this.CreatePlayListsButton.Dock = System.Windows.Forms.DockStyle.Top;
            this.CreatePlayListsButton.FlatAppearance.BorderSize = 0;
            this.CreatePlayListsButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.CreatePlayListsButton.Font = new System.Drawing.Font("Century Gothic", 10F);
            this.CreatePlayListsButton.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.CreatePlayListsButton.Location = new System.Drawing.Point(0, 80);
            this.CreatePlayListsButton.Name = "CreatePlayListsButton";
            this.CreatePlayListsButton.Size = new System.Drawing.Size(233, 40);
            this.CreatePlayListsButton.TabIndex = 2;
            this.CreatePlayListsButton.Text = "PlayLists";
            this.CreatePlayListsButton.UseVisualStyleBackColor = true;
            this.CreatePlayListsButton.Click += new System.EventHandler(this.CreatePlayListsButton_Click);
            // 
            // CreateVideosButton
            // 
            this.CreateVideosButton.Dock = System.Windows.Forms.DockStyle.Top;
            this.CreateVideosButton.FlatAppearance.BorderSize = 0;
            this.CreateVideosButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.CreateVideosButton.Font = new System.Drawing.Font("Century Gothic", 10F);
            this.CreateVideosButton.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.CreateVideosButton.Location = new System.Drawing.Point(0, 40);
            this.CreateVideosButton.Name = "CreateVideosButton";
            this.CreateVideosButton.Size = new System.Drawing.Size(233, 40);
            this.CreateVideosButton.TabIndex = 1;
            this.CreateVideosButton.Text = "Videos";
            this.CreateVideosButton.UseVisualStyleBackColor = true;
            this.CreateVideosButton.Click += new System.EventHandler(this.CreateVideosButton_Click);
            // 
            // CreateSongsButton
            // 
            this.CreateSongsButton.Dock = System.Windows.Forms.DockStyle.Top;
            this.CreateSongsButton.FlatAppearance.BorderSize = 0;
            this.CreateSongsButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.CreateSongsButton.Font = new System.Drawing.Font("Century Gothic", 10F);
            this.CreateSongsButton.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.CreateSongsButton.Location = new System.Drawing.Point(0, 0);
            this.CreateSongsButton.Name = "CreateSongsButton";
            this.CreateSongsButton.Size = new System.Drawing.Size(233, 40);
            this.CreateSongsButton.TabIndex = 0;
            this.CreateSongsButton.Text = "Songs";
            this.CreateSongsButton.UseVisualStyleBackColor = true;
            this.CreateSongsButton.Click += new System.EventHandler(this.CreateSongsButton_Click);
            // 
            // CreateButton
            // 
            this.CreateButton.Dock = System.Windows.Forms.DockStyle.Top;
            this.CreateButton.FlatAppearance.BorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(234)))), ((int)(((byte)(188)))), ((int)(((byte)(45)))));
            this.CreateButton.FlatAppearance.BorderSize = 3;
            this.CreateButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.CreateButton.Font = new System.Drawing.Font("Century Gothic", 10F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.CreateButton.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.CreateButton.Location = new System.Drawing.Point(0, 401);
            this.CreateButton.Name = "CreateButton";
            this.CreateButton.Size = new System.Drawing.Size(233, 45);
            this.CreateButton.TabIndex = 5;
            this.CreateButton.Text = "Create What U Want";
            this.CreateButton.UseVisualStyleBackColor = true;
            this.CreateButton.Click += new System.EventHandler(this.CreateButton_Click);
            // 
            // PlayListsOptionsPanel
            // 
            this.PlayListsOptionsPanel.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(4)))), ((int)(((byte)(20)))), ((int)(((byte)(34)))));
            this.PlayListsOptionsPanel.Controls.Add(this.FavoritePlsButton);
            this.PlayListsOptionsPanel.Controls.Add(this.GlobalPlsButton);
            this.PlayListsOptionsPanel.Controls.Add(this.PrivatePlsButton);
            this.PlayListsOptionsPanel.Dock = System.Windows.Forms.DockStyle.Top;
            this.PlayListsOptionsPanel.Location = new System.Drawing.Point(0, 275);
            this.PlayListsOptionsPanel.Name = "PlayListsOptionsPanel";
            this.PlayListsOptionsPanel.Size = new System.Drawing.Size(233, 126);
            this.PlayListsOptionsPanel.TabIndex = 4;
            // 
            // FavoritePlsButton
            // 
            this.FavoritePlsButton.Dock = System.Windows.Forms.DockStyle.Top;
            this.FavoritePlsButton.FlatAppearance.BorderSize = 0;
            this.FavoritePlsButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.FavoritePlsButton.Font = new System.Drawing.Font("Century Gothic", 10F);
            this.FavoritePlsButton.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.FavoritePlsButton.Location = new System.Drawing.Point(0, 80);
            this.FavoritePlsButton.Name = "FavoritePlsButton";
            this.FavoritePlsButton.Size = new System.Drawing.Size(233, 40);
            this.FavoritePlsButton.TabIndex = 2;
            this.FavoritePlsButton.Text = "Favorite PlayLists";
            this.FavoritePlsButton.UseVisualStyleBackColor = true;
            this.FavoritePlsButton.Click += new System.EventHandler(this.FavoritePlsButton_Click);
            // 
            // GlobalPlsButton
            // 
            this.GlobalPlsButton.Dock = System.Windows.Forms.DockStyle.Top;
            this.GlobalPlsButton.FlatAppearance.BorderSize = 0;
            this.GlobalPlsButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.GlobalPlsButton.Font = new System.Drawing.Font("Century Gothic", 10F);
            this.GlobalPlsButton.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.GlobalPlsButton.Location = new System.Drawing.Point(0, 40);
            this.GlobalPlsButton.Name = "GlobalPlsButton";
            this.GlobalPlsButton.Size = new System.Drawing.Size(233, 40);
            this.GlobalPlsButton.TabIndex = 1;
            this.GlobalPlsButton.Text = "Global PlayLists";
            this.GlobalPlsButton.UseVisualStyleBackColor = true;
            this.GlobalPlsButton.Click += new System.EventHandler(this.GlobalPlsButton_Click);
            // 
            // PrivatePlsButton
            // 
            this.PrivatePlsButton.Dock = System.Windows.Forms.DockStyle.Top;
            this.PrivatePlsButton.FlatAppearance.BorderSize = 0;
            this.PrivatePlsButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.PrivatePlsButton.Font = new System.Drawing.Font("Century Gothic", 10F);
            this.PrivatePlsButton.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.PrivatePlsButton.Location = new System.Drawing.Point(0, 0);
            this.PrivatePlsButton.Name = "PrivatePlsButton";
            this.PrivatePlsButton.Size = new System.Drawing.Size(233, 40);
            this.PrivatePlsButton.TabIndex = 0;
            this.PrivatePlsButton.Text = "Private PlayLists";
            this.PrivatePlsButton.UseVisualStyleBackColor = true;
            this.PrivatePlsButton.Click += new System.EventHandler(this.PrivatePlsButton_Click);
            // 
            // PlayListsButton
            // 
            this.PlayListsButton.Dock = System.Windows.Forms.DockStyle.Top;
            this.PlayListsButton.FlatAppearance.BorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(234)))), ((int)(((byte)(188)))), ((int)(((byte)(45)))));
            this.PlayListsButton.FlatAppearance.BorderSize = 3;
            this.PlayListsButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.PlayListsButton.Font = new System.Drawing.Font("Century Gothic", 10F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.PlayListsButton.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.PlayListsButton.Location = new System.Drawing.Point(0, 230);
            this.PlayListsButton.Name = "PlayListsButton";
            this.PlayListsButton.Size = new System.Drawing.Size(233, 45);
            this.PlayListsButton.TabIndex = 3;
            this.PlayListsButton.Text = "PlayLists";
            this.PlayListsButton.UseVisualStyleBackColor = true;
            this.PlayListsButton.Click += new System.EventHandler(this.PlayListsButton_Click);
            // 
            // MultimediaIOptionsPanel
            // 
            this.MultimediaIOptionsPanel.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(4)))), ((int)(((byte)(20)))), ((int)(((byte)(34)))));
            this.MultimediaIOptionsPanel.Controls.Add(this.VideosButton);
            this.MultimediaIOptionsPanel.Controls.Add(this.SongsButton);
            this.MultimediaIOptionsPanel.Dock = System.Windows.Forms.DockStyle.Top;
            this.MultimediaIOptionsPanel.Location = new System.Drawing.Point(0, 145);
            this.MultimediaIOptionsPanel.Name = "MultimediaIOptionsPanel";
            this.MultimediaIOptionsPanel.Size = new System.Drawing.Size(233, 85);
            this.MultimediaIOptionsPanel.TabIndex = 2;
            // 
            // VideosButton
            // 
            this.VideosButton.Dock = System.Windows.Forms.DockStyle.Top;
            this.VideosButton.FlatAppearance.BorderSize = 0;
            this.VideosButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.VideosButton.Font = new System.Drawing.Font("Century Gothic", 10F);
            this.VideosButton.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.VideosButton.Location = new System.Drawing.Point(0, 40);
            this.VideosButton.Name = "VideosButton";
            this.VideosButton.Size = new System.Drawing.Size(233, 40);
            this.VideosButton.TabIndex = 1;
            this.VideosButton.Text = "Videos";
            this.VideosButton.UseVisualStyleBackColor = true;
            this.VideosButton.Click += new System.EventHandler(this.VideosButton_Click);
            // 
            // SongsButton
            // 
            this.SongsButton.Dock = System.Windows.Forms.DockStyle.Top;
            this.SongsButton.FlatAppearance.BorderSize = 0;
            this.SongsButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.SongsButton.Font = new System.Drawing.Font("Century Gothic", 10F);
            this.SongsButton.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.SongsButton.Location = new System.Drawing.Point(0, 0);
            this.SongsButton.Name = "SongsButton";
            this.SongsButton.Size = new System.Drawing.Size(233, 40);
            this.SongsButton.TabIndex = 0;
            this.SongsButton.Text = "Songs";
            this.SongsButton.UseVisualStyleBackColor = true;
            this.SongsButton.Click += new System.EventHandler(this.SongsButton_Click);
            // 
            // MultimediaButton
            // 
            this.MultimediaButton.Dock = System.Windows.Forms.DockStyle.Top;
            this.MultimediaButton.FlatAppearance.BorderColor = System.Drawing.Color.FromArgb(((int)(((byte)(234)))), ((int)(((byte)(188)))), ((int)(((byte)(45)))));
            this.MultimediaButton.FlatAppearance.BorderSize = 3;
            this.MultimediaButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.MultimediaButton.Font = new System.Drawing.Font("Century Gothic", 10F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.MultimediaButton.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.MultimediaButton.Location = new System.Drawing.Point(0, 100);
            this.MultimediaButton.Name = "MultimediaButton";
            this.MultimediaButton.Size = new System.Drawing.Size(233, 45);
            this.MultimediaButton.TabIndex = 1;
            this.MultimediaButton.Text = "Multimedia";
            this.MultimediaButton.UseVisualStyleBackColor = true;
            this.MultimediaButton.Click += new System.EventHandler(this.MultimediaButton_Click);
            // 
            // LogoFyBuzz
            // 
            this.LogoFyBuzz.Controls.Add(this.pictureBox1);
            this.LogoFyBuzz.Dock = System.Windows.Forms.DockStyle.Top;
            this.LogoFyBuzz.Location = new System.Drawing.Point(0, 0);
            this.LogoFyBuzz.Name = "LogoFyBuzz";
            this.LogoFyBuzz.Size = new System.Drawing.Size(233, 100);
            this.LogoFyBuzz.TabIndex = 0;
            // 
            // pictureBox1
            // 
            this.pictureBox1.Dock = System.Windows.Forms.DockStyle.Left;
            this.pictureBox1.Image = ((System.Drawing.Image)(resources.GetObject("pictureBox1.Image")));
            this.pictureBox1.Location = new System.Drawing.Point(0, 0);
            this.pictureBox1.Name = "pictureBox1";
            this.pictureBox1.Size = new System.Drawing.Size(109, 100);
            this.pictureBox1.SizeMode = System.Windows.Forms.PictureBoxSizeMode.Zoom;
            this.pictureBox1.TabIndex = 0;
            this.pictureBox1.TabStop = false;
            // 
            // PlayerPanel
            // 
            this.PlayerPanel.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(8)))), ((int)(((byte)(8)))), ((int)(((byte)(44)))));
            this.PlayerPanel.Controls.Add(this.RateMediaButton);
            this.PlayerPanel.Controls.Add(this.LikeMediaButton);
            this.PlayerPanel.Controls.Add(this.InfoMediaButton);
            this.PlayerPanel.Controls.Add(this.BackWardMediaButton);
            this.PlayerPanel.Controls.Add(this.ForwardMediaButton);
            this.PlayerPanel.Controls.Add(this.PlayMediaButton);
            this.PlayerPanel.Controls.Add(this.PauseMediaButton);
            this.PlayerPanel.Controls.Add(this.ProgressBarMedia);
            this.PlayerPanel.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.PlayerPanel.Location = new System.Drawing.Point(250, 438);
            this.PlayerPanel.Name = "PlayerPanel";
            this.PlayerPanel.Size = new System.Drawing.Size(684, 123);
            this.PlayerPanel.TabIndex = 1;
            // 
            // RateMediaButton
            // 
            this.RateMediaButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.RateMediaButton.Flip = FontAwesome.Sharp.FlipOrientation.Normal;
            this.RateMediaButton.Font = new System.Drawing.Font("Century Gothic", 11.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.RateMediaButton.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.RateMediaButton.IconChar = FontAwesome.Sharp.IconChar.StarHalfAlt;
            this.RateMediaButton.IconColor = System.Drawing.Color.FromArgb(((int)(((byte)(234)))), ((int)(((byte)(188)))), ((int)(((byte)(45)))));
            this.RateMediaButton.IconSize = 35;
            this.RateMediaButton.ImageAlign = System.Drawing.ContentAlignment.MiddleLeft;
            this.RateMediaButton.Location = new System.Drawing.Point(472, 81);
            this.RateMediaButton.Name = "RateMediaButton";
            this.RateMediaButton.Rotation = 0D;
            this.RateMediaButton.Size = new System.Drawing.Size(178, 39);
            this.RateMediaButton.TabIndex = 8;
            this.RateMediaButton.Text = "Rate Multimedia";
            this.RateMediaButton.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;
            this.RateMediaButton.TextImageRelation = System.Windows.Forms.TextImageRelation.ImageBeforeText;
            this.RateMediaButton.UseVisualStyleBackColor = true;
            // 
            // LikeMediaButton
            // 
            this.LikeMediaButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.LikeMediaButton.Flip = FontAwesome.Sharp.FlipOrientation.Normal;
            this.LikeMediaButton.Font = new System.Drawing.Font("Century Gothic", 11.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.LikeMediaButton.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.LikeMediaButton.IconChar = FontAwesome.Sharp.IconChar.ThumbsUp;
            this.LikeMediaButton.IconColor = System.Drawing.Color.FromArgb(((int)(((byte)(234)))), ((int)(((byte)(188)))), ((int)(((byte)(45)))));
            this.LikeMediaButton.IconSize = 35;
            this.LikeMediaButton.ImageAlign = System.Drawing.ContentAlignment.MiddleLeft;
            this.LikeMediaButton.Location = new System.Drawing.Point(472, 38);
            this.LikeMediaButton.Name = "LikeMediaButton";
            this.LikeMediaButton.Rotation = 0D;
            this.LikeMediaButton.Size = new System.Drawing.Size(178, 39);
            this.LikeMediaButton.TabIndex = 7;
            this.LikeMediaButton.Text = "Like Multimedia";
            this.LikeMediaButton.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;
            this.LikeMediaButton.TextImageRelation = System.Windows.Forms.TextImageRelation.ImageBeforeText;
            this.LikeMediaButton.UseVisualStyleBackColor = true;
            // 
            // InfoMediaButton
            // 
            this.InfoMediaButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.InfoMediaButton.Flip = FontAwesome.Sharp.FlipOrientation.Normal;
            this.InfoMediaButton.Font = new System.Drawing.Font("Century Gothic", 11.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.InfoMediaButton.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.InfoMediaButton.IconChar = FontAwesome.Sharp.IconChar.InfoCircle;
            this.InfoMediaButton.IconColor = System.Drawing.Color.FromArgb(((int)(((byte)(234)))), ((int)(((byte)(188)))), ((int)(((byte)(45)))));
            this.InfoMediaButton.IconSize = 35;
            this.InfoMediaButton.ImageAlign = System.Drawing.ContentAlignment.MiddleLeft;
            this.InfoMediaButton.Location = new System.Drawing.Point(472, 0);
            this.InfoMediaButton.Name = "InfoMediaButton";
            this.InfoMediaButton.Rotation = 0D;
            this.InfoMediaButton.Size = new System.Drawing.Size(178, 39);
            this.InfoMediaButton.TabIndex = 6;
            this.InfoMediaButton.Text = "Multimedia Info";
            this.InfoMediaButton.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;
            this.InfoMediaButton.TextImageRelation = System.Windows.Forms.TextImageRelation.ImageBeforeText;
            this.InfoMediaButton.UseVisualStyleBackColor = true;
            // 
            // BackWardMediaButton
            // 
            this.BackWardMediaButton.FlatAppearance.BorderSize = 0;
            this.BackWardMediaButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.BackWardMediaButton.Flip = FontAwesome.Sharp.FlipOrientation.Normal;
            this.BackWardMediaButton.IconChar = FontAwesome.Sharp.IconChar.Backward;
            this.BackWardMediaButton.IconColor = System.Drawing.Color.FromArgb(((int)(((byte)(234)))), ((int)(((byte)(188)))), ((int)(((byte)(45)))));
            this.BackWardMediaButton.IconSize = 50;
            this.BackWardMediaButton.Location = new System.Drawing.Point(117, 31);
            this.BackWardMediaButton.Name = "BackWardMediaButton";
            this.BackWardMediaButton.Rotation = 0D;
            this.BackWardMediaButton.Size = new System.Drawing.Size(57, 40);
            this.BackWardMediaButton.TabIndex = 5;
            this.BackWardMediaButton.UseVisualStyleBackColor = true;
            // 
            // ForwardMediaButton
            // 
            this.ForwardMediaButton.FlatAppearance.BorderSize = 0;
            this.ForwardMediaButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.ForwardMediaButton.Flip = FontAwesome.Sharp.FlipOrientation.Normal;
            this.ForwardMediaButton.IconChar = FontAwesome.Sharp.IconChar.Forward;
            this.ForwardMediaButton.IconColor = System.Drawing.Color.FromArgb(((int)(((byte)(234)))), ((int)(((byte)(188)))), ((int)(((byte)(45)))));
            this.ForwardMediaButton.IconSize = 50;
            this.ForwardMediaButton.Location = new System.Drawing.Point(297, 28);
            this.ForwardMediaButton.Name = "ForwardMediaButton";
            this.ForwardMediaButton.Rotation = 0D;
            this.ForwardMediaButton.Size = new System.Drawing.Size(57, 45);
            this.ForwardMediaButton.TabIndex = 4;
            this.ForwardMediaButton.UseVisualStyleBackColor = true;
            // 
            // PlayMediaButton
            // 
            this.PlayMediaButton.FlatAppearance.BorderSize = 0;
            this.PlayMediaButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.PlayMediaButton.Flip = FontAwesome.Sharp.FlipOrientation.Normal;
            this.PlayMediaButton.IconChar = FontAwesome.Sharp.IconChar.Play;
            this.PlayMediaButton.IconColor = System.Drawing.Color.FromArgb(((int)(((byte)(234)))), ((int)(((byte)(188)))), ((int)(((byte)(45)))));
            this.PlayMediaButton.IconSize = 50;
            this.PlayMediaButton.Location = new System.Drawing.Point(234, 26);
            this.PlayMediaButton.Name = "PlayMediaButton";
            this.PlayMediaButton.Rotation = 0D;
            this.PlayMediaButton.Size = new System.Drawing.Size(57, 51);
            this.PlayMediaButton.TabIndex = 2;
            this.PlayMediaButton.UseVisualStyleBackColor = true;
            // 
            // PauseMediaButton
            // 
            this.PauseMediaButton.FlatAppearance.BorderSize = 0;
            this.PauseMediaButton.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.PauseMediaButton.Flip = FontAwesome.Sharp.FlipOrientation.Normal;
            this.PauseMediaButton.IconChar = FontAwesome.Sharp.IconChar.Pause;
            this.PauseMediaButton.IconColor = System.Drawing.Color.FromArgb(((int)(((byte)(234)))), ((int)(((byte)(188)))), ((int)(((byte)(45)))));
            this.PauseMediaButton.IconSize = 50;
            this.PauseMediaButton.Location = new System.Drawing.Point(180, 28);
            this.PauseMediaButton.Name = "PauseMediaButton";
            this.PauseMediaButton.Rotation = 0D;
            this.PauseMediaButton.Size = new System.Drawing.Size(48, 46);
            this.PauseMediaButton.TabIndex = 1;
            this.PauseMediaButton.UseVisualStyleBackColor = true;
            // 
            // ProgressBarMedia
            // 
            this.ProgressBarMedia.Cursor = System.Windows.Forms.Cursors.Cross;
            this.ProgressBarMedia.Location = new System.Drawing.Point(59, 88);
            this.ProgressBarMedia.Name = "ProgressBarMedia";
            this.ProgressBarMedia.Size = new System.Drawing.Size(353, 10);
            this.ProgressBarMedia.TabIndex = 0;
            // 
            // GeneralPanel
            // 
            this.GeneralPanel.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(11)))), ((int)(((byte)(20)))), ((int)(((byte)(35)))));
            this.GeneralPanel.Controls.Add(this.textBox3);
            this.GeneralPanel.Controls.Add(this.iconButton1);
            this.GeneralPanel.Controls.Add(this.textBox1);
            this.GeneralPanel.Controls.Add(this.pictureBox2);
            this.GeneralPanel.Dock = System.Windows.Forms.DockStyle.Fill;
            this.GeneralPanel.Location = new System.Drawing.Point(250, 0);
            this.GeneralPanel.Name = "GeneralPanel";
            this.GeneralPanel.Size = new System.Drawing.Size(684, 438);
            this.GeneralPanel.TabIndex = 2;
            this.GeneralPanel.Paint += new System.Windows.Forms.PaintEventHandler(this.GeneralPanel_Paint);
            // 
            // pictureBox2
            // 
            this.pictureBox2.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.pictureBox2.Image = ((System.Drawing.Image)(resources.GetObject("pictureBox2.Image")));
            this.pictureBox2.Location = new System.Drawing.Point(197, 35);
            this.pictureBox2.Name = "pictureBox2";
            this.pictureBox2.Size = new System.Drawing.Size(266, 210);
            this.pictureBox2.SizeMode = System.Windows.Forms.PictureBoxSizeMode.StretchImage;
            this.pictureBox2.TabIndex = 0;
            this.pictureBox2.TabStop = false;
            // 
            // contextMenuStrip1
            // 
            this.contextMenuStrip1.Name = "contextMenuStrip1";
            this.contextMenuStrip1.Size = new System.Drawing.Size(61, 4);
            // 
            // textBox1
            // 
            this.textBox1.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.textBox1.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(123)))), ((int)(((byte)(116)))), ((int)(((byte)(90)))));
            this.textBox1.Location = new System.Drawing.Point(197, 275);
            this.textBox1.Multiline = true;
            this.textBox1.Name = "textBox1";
            this.textBox1.Size = new System.Drawing.Size(266, 29);
            this.textBox1.TabIndex = 1;
            this.textBox1.Text = "\r\n";
            // 
            // iconButton1
            // 
            this.iconButton1.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.iconButton1.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.iconButton1.Flip = FontAwesome.Sharp.FlipOrientation.Normal;
            this.iconButton1.Font = new System.Drawing.Font("Century Gothic", 14.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.iconButton1.ForeColor = System.Drawing.SystemColors.ButtonFace;
            this.iconButton1.IconChar = FontAwesome.Sharp.IconChar.IdCard;
            this.iconButton1.IconColor = System.Drawing.Color.FromArgb(((int)(((byte)(234)))), ((int)(((byte)(188)))), ((int)(((byte)(45)))));
            this.iconButton1.IconSize = 30;
            this.iconButton1.Location = new System.Drawing.Point(258, 362);
            this.iconButton1.Name = "iconButton1";
            this.iconButton1.Rotation = 0D;
            this.iconButton1.Size = new System.Drawing.Size(141, 39);
            this.iconButton1.TabIndex = 3;
            this.iconButton1.Text = "LOG IN\r\n";
            this.iconButton1.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;
            this.iconButton1.TextImageRelation = System.Windows.Forms.TextImageRelation.ImageBeforeText;
            this.iconButton1.UseVisualStyleBackColor = true;
            // 
            // textBox3
            // 
            this.textBox3.Anchor = System.Windows.Forms.AnchorStyles.None;
            this.textBox3.BackColor = System.Drawing.Color.FromArgb(((int)(((byte)(123)))), ((int)(((byte)(116)))), ((int)(((byte)(90)))));
            this.textBox3.Location = new System.Drawing.Point(197, 315);
            this.textBox3.Multiline = true;
            this.textBox3.Name = "textBox3";
            this.textBox3.Size = new System.Drawing.Size(266, 29);
            this.textBox3.TabIndex = 4;
            this.textBox3.Text = "\r\n";
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(934, 561);
            this.Controls.Add(this.GeneralPanel);
            this.Controls.Add(this.PlayerPanel);
            this.Controls.Add(this.SideMenu);
            this.Font = new System.Drawing.Font("Microsoft Sans Serif", 10F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Margin = new System.Windows.Forms.Padding(4);
            this.MinimumSize = new System.Drawing.Size(950, 600);
            this.Name = "Form1";
            this.Text = "Form1";
            this.SideMenu.ResumeLayout(false);
            this.CreateOptionsPanel.ResumeLayout(false);
            this.PlayListsOptionsPanel.ResumeLayout(false);
            this.MultimediaIOptionsPanel.ResumeLayout(false);
            this.LogoFyBuzz.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox1)).EndInit();
            this.PlayerPanel.ResumeLayout(false);
            this.GeneralPanel.ResumeLayout(false);
            this.GeneralPanel.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox2)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Panel SideMenu;
        private System.Windows.Forms.Panel LogoFyBuzz;
        private System.Windows.Forms.Panel MultimediaIOptionsPanel;
        private System.Windows.Forms.Button VideosButton;
        private System.Windows.Forms.Button SongsButton;
        private System.Windows.Forms.Button MultimediaButton;
        private System.Windows.Forms.Panel CreateOptionsPanel;
        private System.Windows.Forms.Button CreateProfilesButton;
        private System.Windows.Forms.Button CreatePlayListsButton;
        private System.Windows.Forms.Button CreateVideosButton;
        private System.Windows.Forms.Button CreateSongsButton;
        private System.Windows.Forms.Button CreateButton;
        private System.Windows.Forms.Panel PlayListsOptionsPanel;
        private System.Windows.Forms.Button FavoritePlsButton;
        private System.Windows.Forms.Button GlobalPlsButton;
        private System.Windows.Forms.Button PrivatePlsButton;
        private System.Windows.Forms.Button PlayListsButton;
        private System.Windows.Forms.Button AboutUsButton;
        private System.Windows.Forms.Panel PlayerPanel;
        private System.Windows.Forms.Panel GeneralPanel;
        private System.Windows.Forms.PictureBox pictureBox1;
        private FontAwesome.Sharp.IconButton RateMediaButton;
        private FontAwesome.Sharp.IconButton LikeMediaButton;
        private FontAwesome.Sharp.IconButton InfoMediaButton;
        private FontAwesome.Sharp.IconButton BackWardMediaButton;
        private FontAwesome.Sharp.IconButton ForwardMediaButton;
        private FontAwesome.Sharp.IconButton PlayMediaButton;
        private FontAwesome.Sharp.IconButton PauseMediaButton;
        private System.Windows.Forms.ProgressBar ProgressBarMedia;
        private System.Windows.Forms.PictureBox pictureBox2;
        private System.Windows.Forms.ContextMenuStrip contextMenuStrip1;
        private System.Windows.Forms.TextBox textBox3;
        private FontAwesome.Sharp.IconButton iconButton1;
        private System.Windows.Forms.TextBox textBox1;
    }
}

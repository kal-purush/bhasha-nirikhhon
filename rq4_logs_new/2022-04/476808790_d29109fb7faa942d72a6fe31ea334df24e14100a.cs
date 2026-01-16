namespace NotSteam
{
    partial class Game
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(Game));
            this.lbNameGame = new System.Windows.Forms.Label();
            this.pbGame = new System.Windows.Forms.PictureBox();
            this.btBuyGame = new System.Windows.Forms.Button();
            this.tbGame = new System.Windows.Forms.RichTextBox();
            this.lbDevGame = new System.Windows.Forms.Label();
            this.lbRelease = new System.Windows.Forms.Label();
            this.lbGameNameBuy = new System.Windows.Forms.Label();
            this.tableLayoutPanel1 = new System.Windows.Forms.TableLayoutPanel();
            this.lbMissingGame = new System.Windows.Forms.Label();
            this.imageList1 = new System.Windows.Forms.ImageList(this.components);
            ((System.ComponentModel.ISupportInitialize)(this.pbGame)).BeginInit();
            this.tableLayoutPanel1.SuspendLayout();
            this.SuspendLayout();
            // 
            // lbNameGame
            // 
            this.lbNameGame.AutoSize = true;
            this.lbNameGame.Font = new System.Drawing.Font("Microsoft Sans Serif", 15F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lbNameGame.Location = new System.Drawing.Point(15, 90);
            this.lbNameGame.Name = "lbNameGame";
            this.lbNameGame.Size = new System.Drawing.Size(64, 25);
            this.lbNameGame.TabIndex = 0;
            this.lbNameGame.Text = "label1";
            this.lbNameGame.UseMnemonic = false;
            // 
            // pbGame
            // 
            this.pbGame.Location = new System.Drawing.Point(12, 120);
            this.pbGame.Name = "pbGame";
            this.pbGame.Size = new System.Drawing.Size(256, 199);
            this.pbGame.TabIndex = 1;
            this.pbGame.TabStop = false;
            // 
            // btBuyGame
            // 
            this.btBuyGame.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Right)));
            this.btBuyGame.Location = new System.Drawing.Point(512, 49);
            this.btBuyGame.Name = "btBuyGame";
            this.btBuyGame.Size = new System.Drawing.Size(75, 23);
            this.btBuyGame.TabIndex = 2;
            this.btBuyGame.Text = "button1";
            this.btBuyGame.UseVisualStyleBackColor = true;
            this.btBuyGame.Click += new System.EventHandler(this.btBuyGame_Click);
            // 
            // tbGame
            // 
            this.tbGame.Location = new System.Drawing.Point(317, 120);
            this.tbGame.Name = "tbGame";
            this.tbGame.Size = new System.Drawing.Size(397, 126);
            this.tbGame.TabIndex = 3;
            this.tbGame.Text = "";
            // 
            // lbDevGame
            // 
            this.lbDevGame.AutoSize = true;
            this.lbDevGame.Location = new System.Drawing.Point(314, 258);
            this.lbDevGame.Name = "lbDevGame";
            this.lbDevGame.Size = new System.Drawing.Size(35, 13);
            this.lbDevGame.TabIndex = 4;
            this.lbDevGame.Text = "label1";
            // 
            // lbRelease
            // 
            this.lbRelease.AutoSize = true;
            this.lbRelease.Location = new System.Drawing.Point(314, 286);
            this.lbRelease.Name = "lbRelease";
            this.lbRelease.Size = new System.Drawing.Size(35, 13);
            this.lbRelease.TabIndex = 5;
            this.lbRelease.Text = "label1";
            // 
            // lbGameNameBuy
            // 
            this.lbGameNameBuy.AutoSize = true;
            this.lbGameNameBuy.Location = new System.Drawing.Point(3, 0);
            this.lbGameNameBuy.Name = "lbGameNameBuy";
            this.lbGameNameBuy.Size = new System.Drawing.Size(35, 13);
            this.lbGameNameBuy.TabIndex = 6;
            this.lbGameNameBuy.Text = "label1";
            // 
            // tableLayoutPanel1
            // 
            this.tableLayoutPanel1.ColumnCount = 2;
            this.tableLayoutPanel1.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 50F));
            this.tableLayoutPanel1.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 50F));
            this.tableLayoutPanel1.Controls.Add(this.btBuyGame, 1, 1);
            this.tableLayoutPanel1.Controls.Add(this.lbGameNameBuy, 0, 0);
            this.tableLayoutPanel1.Location = new System.Drawing.Point(95, 363);
            this.tableLayoutPanel1.Name = "tableLayoutPanel1";
            this.tableLayoutPanel1.RowCount = 2;
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 50F));
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 50F));
            this.tableLayoutPanel1.Size = new System.Drawing.Size(590, 75);
            this.tableLayoutPanel1.TabIndex = 7;
            // 
            // lbMissingGame
            // 
            this.lbMissingGame.AutoSize = true;
            this.lbMissingGame.Location = new System.Drawing.Point(92, 322);
            this.lbMissingGame.Name = "lbMissingGame";
            this.lbMissingGame.Size = new System.Drawing.Size(74, 13);
            this.lbMissingGame.TabIndex = 8;
            this.lbMissingGame.Text = "Missing Image";
            this.lbMissingGame.Visible = false;
            // 
            // imageList1
            // 
            this.imageList1.ImageStream = ((System.Windows.Forms.ImageListStreamer)(resources.GetObject("imageList1.ImageStream")));
            this.imageList1.TransparentColor = System.Drawing.Color.Transparent;
            this.imageList1.Images.SetKeyName(0, "Steam_service-Logo.wine.png");
            this.imageList1.Images.SetKeyName(1, "back-arrow-icon-image-free-download-searchpng-arrow-gray-world-of-warcraft-transp" +
        "arent-png-530549_ccexpress.png");
            this.imageList1.Images.SetKeyName(2, "gta 5.jpg");
            this.imageList1.Images.SetKeyName(3, "bloons-td6-purple.jpg");
            this.imageList1.Images.SetKeyName(4, "Dying_Light.jpg");
            this.imageList1.Images.SetKeyName(5, "new world.jpg");
            this.imageList1.Images.SetKeyName(6, "cs1.6.jpg");
            this.imageList1.Images.SetKeyName(7, "pubg.jpg");
            this.imageList1.Images.SetKeyName(8, "sims4.jpg");
            this.imageList1.Images.SetKeyName(9, "csgo.png");
            this.imageList1.Images.SetKeyName(10, "Farming simulator.jpg");
            this.imageList1.Images.SetKeyName(11, "who\'s your daddy.jpg");
            this.imageList1.Images.SetKeyName(12, "unturned.jpg");
            // 
            // Game
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 450);
            this.Controls.Add(this.lbMissingGame);
            this.Controls.Add(this.tableLayoutPanel1);
            this.Controls.Add(this.lbRelease);
            this.Controls.Add(this.lbDevGame);
            this.Controls.Add(this.tbGame);
            this.Controls.Add(this.pbGame);
            this.Controls.Add(this.lbNameGame);
            this.Name = "Game";
            this.Text = "Game";
            ((System.ComponentModel.ISupportInitialize)(this.pbGame)).EndInit();
            this.tableLayoutPanel1.ResumeLayout(false);
            this.tableLayoutPanel1.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label lbNameGame;
        private System.Windows.Forms.PictureBox pbGame;
        private System.Windows.Forms.Button btBuyGame;
        private System.Windows.Forms.RichTextBox tbGame;
        private System.Windows.Forms.Label lbDevGame;
        private System.Windows.Forms.Label lbRelease;
        private System.Windows.Forms.Label lbGameNameBuy;
        private System.Windows.Forms.TableLayoutPanel tableLayoutPanel1;
        private System.Windows.Forms.Label lbMissingGame;
        private System.Windows.Forms.ImageList imageList1;
    }
}
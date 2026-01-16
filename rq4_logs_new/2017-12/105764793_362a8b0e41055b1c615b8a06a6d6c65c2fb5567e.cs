namespace WF_FileDropBoard {

    partial class Setting_Part_Window
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing) {
            if (disposing && ( components != null )) {
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
            this.Setting_WindowTLP = new System.Windows.Forms.TableLayoutPanel();
            this.DescLabel = new System.Windows.Forms.Label();
            this.Settings_Tile_Panel = new System.Windows.Forms.Panel();
            this.AutoHideDescLabel = new System.Windows.Forms.Label();
            this.TopMostCB = new System.Windows.Forms.CheckBox();
            this.AutoHideSecNUD = new System.Windows.Forms.NumericUpDown();
            this.AutoHideSecDesc = new System.Windows.Forms.Label();
            this.AutoHideCB = new System.Windows.Forms.CheckBox();
            this.PosSecSelCB = new System.Windows.Forms.ComboBox();
            this.PosSecDescLabel = new System.Windows.Forms.Label();
            this.Setting_WindowTLP.SuspendLayout();
            this.Settings_Tile_Panel.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.AutoHideSecNUD)).BeginInit();
            this.SuspendLayout();
            // 
            // Setting_WindowTLP
            // 
            this.Setting_WindowTLP.ColumnCount = 1;
            this.Setting_WindowTLP.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.Setting_WindowTLP.Controls.Add(this.DescLabel, 0, 0);
            this.Setting_WindowTLP.Controls.Add(this.Settings_Tile_Panel, 0, 1);
            this.Setting_WindowTLP.Dock = System.Windows.Forms.DockStyle.Fill;
            this.Setting_WindowTLP.Location = new System.Drawing.Point(0, 0);
            this.Setting_WindowTLP.Name = "Setting_WindowTLP";
            this.Setting_WindowTLP.RowCount = 2;
            this.Setting_WindowTLP.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Absolute, 28F));
            this.Setting_WindowTLP.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.Setting_WindowTLP.Size = new System.Drawing.Size(284, 261);
            this.Setting_WindowTLP.TabIndex = 3;
            // 
            // DescLabel
            // 
            this.DescLabel.AutoSize = true;
            this.DescLabel.Dock = System.Windows.Forms.DockStyle.Fill;
            this.DescLabel.Font = new System.Drawing.Font("メイリオ", 11F);
            this.DescLabel.Location = new System.Drawing.Point(3, 0);
            this.DescLabel.Name = "DescLabel";
            this.DescLabel.Size = new System.Drawing.Size(278, 28);
            this.DescLabel.TabIndex = 0;
            this.DescLabel.Text = "ウィンドウ";
            this.DescLabel.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;
            // 
            // Settings_Tile_Panel
            // 
            this.Settings_Tile_Panel.AutoScroll = true;
            this.Settings_Tile_Panel.Controls.Add(this.PosSecDescLabel);
            this.Settings_Tile_Panel.Controls.Add(this.PosSecSelCB);
            this.Settings_Tile_Panel.Controls.Add(this.AutoHideDescLabel);
            this.Settings_Tile_Panel.Controls.Add(this.TopMostCB);
            this.Settings_Tile_Panel.Controls.Add(this.AutoHideSecNUD);
            this.Settings_Tile_Panel.Controls.Add(this.AutoHideSecDesc);
            this.Settings_Tile_Panel.Controls.Add(this.AutoHideCB);
            this.Settings_Tile_Panel.Dock = System.Windows.Forms.DockStyle.Fill;
            this.Settings_Tile_Panel.Location = new System.Drawing.Point(0, 28);
            this.Settings_Tile_Panel.Margin = new System.Windows.Forms.Padding(0);
            this.Settings_Tile_Panel.Name = "Settings_Tile_Panel";
            this.Settings_Tile_Panel.Size = new System.Drawing.Size(284, 233);
            this.Settings_Tile_Panel.TabIndex = 1;
            // 
            // AutoHideDescLabel
            // 
            this.AutoHideDescLabel.Location = new System.Drawing.Point(3, 119);
            this.AutoHideDescLabel.Name = "AutoHideDescLabel";
            this.AutoHideDescLabel.Size = new System.Drawing.Size(269, 35);
            this.AutoHideDescLabel.TabIndex = 6;
            this.AutoHideDescLabel.Text = "※デスクトップの端にマウスを移動させると、\r\nウィンドウが再び表示されます。";
            // 
            // TopMostCB
            // 
            this.TopMostCB.AutoSize = true;
            this.TopMostCB.Location = new System.Drawing.Point(3, 100);
            this.TopMostCB.Name = "TopMostCB";
            this.TopMostCB.Size = new System.Drawing.Size(164, 16);
            this.TopMostCB.TabIndex = 5;
            this.TopMostCB.Text = "ウィンドウを最前面に配置する";
            this.TopMostCB.UseVisualStyleBackColor = true;
            this.TopMostCB.CheckedChanged += new System.EventHandler(this.TopMostCB_CheckedChanged);
            // 
            // AutoHideSecNUD
            // 
            this.AutoHideSecNUD.AutoSize = true;
            this.AutoHideSecNUD.Location = new System.Drawing.Point(3, 37);
            this.AutoHideSecNUD.Maximum = new decimal(new int[] {
            180,
            0,
            0,
            0});
            this.AutoHideSecNUD.Minimum = new decimal(new int[] {
            1,
            0,
            0,
            0});
            this.AutoHideSecNUD.Name = "AutoHideSecNUD";
            this.AutoHideSecNUD.Size = new System.Drawing.Size(120, 19);
            this.AutoHideSecNUD.TabIndex = 4;
            this.AutoHideSecNUD.TextAlign = System.Windows.Forms.HorizontalAlignment.Right;
            this.AutoHideSecNUD.Value = new decimal(new int[] {
            1,
            0,
            0,
            0});
            this.AutoHideSecNUD.ValueChanged += new System.EventHandler(this.AutoHideSecNUD_ValueChanged);
            // 
            // AutoHideSecDesc
            // 
            this.AutoHideSecDesc.AutoSize = true;
            this.AutoHideSecDesc.Location = new System.Drawing.Point(3, 22);
            this.AutoHideSecDesc.Name = "AutoHideSecDesc";
            this.AutoHideSecDesc.Size = new System.Drawing.Size(125, 12);
            this.AutoHideSecDesc.TabIndex = 3;
            this.AutoHideSecDesc.Text = "自動的に隠すまでの秒数";
            // 
            // AutoHideCB
            // 
            this.AutoHideCB.AutoSize = true;
            this.AutoHideCB.Location = new System.Drawing.Point(0, 3);
            this.AutoHideCB.Name = "AutoHideCB";
            this.AutoHideCB.Padding = new System.Windows.Forms.Padding(3, 0, 0, 0);
            this.AutoHideCB.Size = new System.Drawing.Size(146, 16);
            this.AutoHideCB.TabIndex = 1;
            this.AutoHideCB.Text = "ウィンドウを自動的に隠す";
            this.AutoHideCB.UseVisualStyleBackColor = true;
            this.AutoHideCB.CheckedChanged += new System.EventHandler(this.AutoHideCB_CheckedChanged);
            // 
            // PosSecSelCB
            // 
            this.PosSecSelCB.AutoCompleteMode = System.Windows.Forms.AutoCompleteMode.Suggest;
            this.PosSecSelCB.AutoCompleteSource = System.Windows.Forms.AutoCompleteSource.ListItems;
            this.PosSecSelCB.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.PosSecSelCB.FormattingEnabled = true;
            this.PosSecSelCB.Items.AddRange(new object[] {
            "左上隅",
            "左下隅",
            "右上隅",
            "右下隅"});
            this.PosSecSelCB.Location = new System.Drawing.Point(5, 74);
            this.PosSecSelCB.Name = "PosSecSelCB";
            this.PosSecSelCB.Size = new System.Drawing.Size(121, 20);
            this.PosSecSelCB.TabIndex = 7;
            this.PosSecSelCB.SelectedIndexChanged += new System.EventHandler(this.PosSecSelCB_SelectedIndexChanged);
            // 
            // PosSecDescLabel
            // 
            this.PosSecDescLabel.AutoSize = true;
            this.PosSecDescLabel.Location = new System.Drawing.Point(3, 59);
            this.PosSecDescLabel.Name = "PosSecDescLabel";
            this.PosSecDescLabel.Size = new System.Drawing.Size(124, 12);
            this.PosSecDescLabel.TabIndex = 8;
            this.PosSecDescLabel.Text = "ウィンドウを配置する場所";
            // 
            // Setting_Part_Window
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 12F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(284, 261);
            this.Controls.Add(this.Setting_WindowTLP);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.None;
            this.Name = "Setting_Part_Window";
            this.Text = "Setting_Part_AutoHide";
            this.Load += new System.EventHandler(this.Setting_Part_Window_Load);
            this.Setting_WindowTLP.ResumeLayout(false);
            this.Setting_WindowTLP.PerformLayout();
            this.Settings_Tile_Panel.ResumeLayout(false);
            this.Settings_Tile_Panel.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.AutoHideSecNUD)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.TableLayoutPanel Setting_WindowTLP;
        private System.Windows.Forms.Label DescLabel;
        private System.Windows.Forms.Panel Settings_Tile_Panel;
        private System.Windows.Forms.Label AutoHideSecDesc;
        private System.Windows.Forms.CheckBox AutoHideCB;
        private System.Windows.Forms.NumericUpDown AutoHideSecNUD;
        private System.Windows.Forms.CheckBox TopMostCB;
        private System.Windows.Forms.Label AutoHideDescLabel;
        private System.Windows.Forms.Label PosSecDescLabel;
        private System.Windows.Forms.ComboBox PosSecSelCB;
    }
}
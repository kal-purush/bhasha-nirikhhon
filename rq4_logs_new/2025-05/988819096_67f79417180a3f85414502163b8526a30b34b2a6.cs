namespace InventoryApp
{
    partial class MainApp
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(MainApp));
            this.materialTabControl1 = new MaterialSkin.Controls.MaterialTabControl();
            this.Dashboard_MenuBtn = new System.Windows.Forms.TabPage();
            this.Storage_MenuBtn = new System.Windows.Forms.TabPage();
            this.Settings_MenuBtn = new System.Windows.Forms.TabPage();
            this.tabsImageList = new System.Windows.Forms.ImageList(this.components);
            this.materialTabControl1.SuspendLayout();
            this.SuspendLayout();
            // 
            // materialTabControl1
            // 
            this.materialTabControl1.Controls.Add(this.Dashboard_MenuBtn);
            this.materialTabControl1.Controls.Add(this.Storage_MenuBtn);
            this.materialTabControl1.Controls.Add(this.Settings_MenuBtn);
            this.materialTabControl1.Depth = 0;
            resources.ApplyResources(this.materialTabControl1, "materialTabControl1");
            this.materialTabControl1.ImageList = this.tabsImageList;
            this.materialTabControl1.MouseState = MaterialSkin.MouseState.HOVER;
            this.materialTabControl1.Multiline = true;
            this.materialTabControl1.Name = "materialTabControl1";
            this.materialTabControl1.SelectedIndex = 0;
            // 
            // Dashboard_MenuBtn
            // 
            resources.ApplyResources(this.Dashboard_MenuBtn, "Dashboard_MenuBtn");
            this.Dashboard_MenuBtn.Name = "Dashboard_MenuBtn";
            this.Dashboard_MenuBtn.UseVisualStyleBackColor = true;
            // 
            // Storage_MenuBtn
            // 
            resources.ApplyResources(this.Storage_MenuBtn, "Storage_MenuBtn");
            this.Storage_MenuBtn.Name = "Storage_MenuBtn";
            this.Storage_MenuBtn.UseVisualStyleBackColor = true;
            // 
            // Settings_MenuBtn
            // 
            resources.ApplyResources(this.Settings_MenuBtn, "Settings_MenuBtn");
            this.Settings_MenuBtn.Name = "Settings_MenuBtn";
            this.Settings_MenuBtn.UseVisualStyleBackColor = true;
            // 
            // tabsImageList
            // 
            this.tabsImageList.ImageStream = ((System.Windows.Forms.ImageListStreamer)(resources.GetObject("tabsImageList.ImageStream")));
            this.tabsImageList.TransparentColor = System.Drawing.Color.Transparent;
            this.tabsImageList.Images.SetKeyName(0, "icons8-dashboard-24.png");
            this.tabsImageList.Images.SetKeyName(1, "icons8-settings-24.png");
            this.tabsImageList.Images.SetKeyName(2, "icons8-storage-24.png");
            this.tabsImageList.Images.SetKeyName(3, "menu.png");
            // 
            // MainApp
            // 
            resources.ApplyResources(this, "$this");
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.materialTabControl1);
            this.DrawerAutoShow = true;
            this.DrawerShowIconsWhenHidden = true;
            this.DrawerTabControl = this.materialTabControl1;
            this.DrawerWidth = 270;
            this.Name = "MainApp";
            this.WindowState = System.Windows.Forms.FormWindowState.Maximized;
            this.materialTabControl1.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion
        private MaterialSkin.Controls.MaterialTabControl materialTabControl1;
        private System.Windows.Forms.TabPage Dashboard_MenuBtn;
        private System.Windows.Forms.TabPage Storage_MenuBtn;
        private System.Windows.Forms.TabPage Settings_MenuBtn;
        private System.Windows.Forms.ImageList tabsImageList;
    }
}

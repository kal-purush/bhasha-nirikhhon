namespace TCM_Pharmex
{
    partial class FrmLogin
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
            this.lbluser = new MetroFramework.Controls.MetroLabel();
            this.lblpass = new MetroFramework.Controls.MetroLabel();
            this.txtbuser = new MetroFramework.Controls.MetroTextBox();
            this.txtbpas = new MetroFramework.Controls.MetroTextBox();
            this.btnlog = new MetroFramework.Controls.MetroButton();
            this.btnlogout = new MetroFramework.Controls.MetroButton();
            this.lblesqas = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // lbluser
            // 
            this.lbluser.AutoSize = true;
            this.lbluser.Location = new System.Drawing.Point(206, 58);
            this.lbluser.Name = "lbluser";
            this.lbluser.Size = new System.Drawing.Size(56, 19);
            this.lbluser.TabIndex = 0;
            this.lbluser.Text = "Usuário:";
            // 
            // lblpass
            // 
            this.lblpass.AutoSize = true;
            this.lblpass.Location = new System.Drawing.Point(206, 121);
            this.lblpass.Name = "lblpass";
            this.lblpass.Size = new System.Drawing.Size(47, 19);
            this.lblpass.TabIndex = 1;
            this.lblpass.Text = "Senha:";
            // 
            // txtbuser
            // 
            // 
            // 
            // 
            this.txtbuser.CustomButton.Image = null;
            this.txtbuser.CustomButton.Location = new System.Drawing.Point(199, 1);
            this.txtbuser.CustomButton.Name = "";
            this.txtbuser.CustomButton.Size = new System.Drawing.Size(21, 21);
            this.txtbuser.CustomButton.Style = MetroFramework.MetroColorStyle.Blue;
            this.txtbuser.CustomButton.TabIndex = 1;
            this.txtbuser.CustomButton.Theme = MetroFramework.MetroThemeStyle.Light;
            this.txtbuser.CustomButton.UseSelectable = true;
            this.txtbuser.CustomButton.Visible = false;
            this.txtbuser.Lines = new string[0];
            this.txtbuser.Location = new System.Drawing.Point(206, 80);
            this.txtbuser.MaxLength = 32767;
            this.txtbuser.Name = "txtbuser";
            this.txtbuser.PasswordChar = '\0';
            this.txtbuser.ScrollBars = System.Windows.Forms.ScrollBars.None;
            this.txtbuser.SelectedText = "";
            this.txtbuser.SelectionLength = 0;
            this.txtbuser.SelectionStart = 0;
            this.txtbuser.ShortcutsEnabled = true;
            this.txtbuser.Size = new System.Drawing.Size(221, 23);
            this.txtbuser.TabIndex = 2;
            this.txtbuser.UseSelectable = true;
            this.txtbuser.WaterMarkColor = System.Drawing.Color.FromArgb(((int)(((byte)(109)))), ((int)(((byte)(109)))), ((int)(((byte)(109)))));
            this.txtbuser.WaterMarkFont = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Pixel);
            // 
            // txtbpas
            // 
            // 
            // 
            // 
            this.txtbpas.CustomButton.Image = null;
            this.txtbpas.CustomButton.Location = new System.Drawing.Point(199, 1);
            this.txtbpas.CustomButton.Name = "";
            this.txtbpas.CustomButton.Size = new System.Drawing.Size(21, 21);
            this.txtbpas.CustomButton.Style = MetroFramework.MetroColorStyle.Blue;
            this.txtbpas.CustomButton.TabIndex = 1;
            this.txtbpas.CustomButton.Theme = MetroFramework.MetroThemeStyle.Light;
            this.txtbpas.CustomButton.UseSelectable = true;
            this.txtbpas.CustomButton.Visible = false;
            this.txtbpas.Lines = new string[0];
            this.txtbpas.Location = new System.Drawing.Point(206, 143);
            this.txtbpas.MaxLength = 32767;
            this.txtbpas.Name = "txtbpas";
            this.txtbpas.PasswordChar = '*';
            this.txtbpas.ScrollBars = System.Windows.Forms.ScrollBars.None;
            this.txtbpas.SelectedText = "";
            this.txtbpas.SelectionLength = 0;
            this.txtbpas.SelectionStart = 0;
            this.txtbpas.ShortcutsEnabled = true;
            this.txtbpas.Size = new System.Drawing.Size(221, 23);
            this.txtbpas.TabIndex = 3;
            this.txtbpas.UseSelectable = true;
            this.txtbpas.WaterMarkColor = System.Drawing.Color.FromArgb(((int)(((byte)(109)))), ((int)(((byte)(109)))), ((int)(((byte)(109)))));
            this.txtbpas.WaterMarkFont = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Pixel);
            // 
            // btnlog
            // 
            this.btnlog.Location = new System.Drawing.Point(206, 234);
            this.btnlog.Name = "btnlog";
            this.btnlog.Size = new System.Drawing.Size(75, 23);
            this.btnlog.TabIndex = 4;
            this.btnlog.Text = "Entrar";
            this.btnlog.UseSelectable = true;
            // 
            // btnlogout
            // 
            this.btnlogout.Location = new System.Drawing.Point(352, 234);
            this.btnlogout.Name = "btnlogout";
            this.btnlogout.Size = new System.Drawing.Size(75, 23);
            this.btnlogout.TabIndex = 5;
            this.btnlogout.Text = "Sair";
            this.btnlogout.UseSelectable = true;
            // 
            // lblesqas
            // 
            this.lblesqas.AutoSize = true;
            this.lblesqas.Font = new System.Drawing.Font("Arial Narrow", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblesqas.ForeColor = System.Drawing.Color.Blue;
            this.lblesqas.Location = new System.Drawing.Point(349, 169);
            this.lblesqas.Name = "lblesqas";
            this.lblesqas.Size = new System.Drawing.Size(80, 15);
            this.lblesqas.TabIndex = 6;
            this.lblesqas.Text = "Esqueci a Senha.";
            // 
            // FrmLogin
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(450, 300);
            this.ControlBox = false;
            this.Controls.Add(this.lblesqas);
            this.Controls.Add(this.btnlogout);
            this.Controls.Add(this.btnlog);
            this.Controls.Add(this.txtbpas);
            this.Controls.Add(this.txtbuser);
            this.Controls.Add(this.lblpass);
            this.Controls.Add(this.lbluser);
            this.Movable = false;
            this.Name = "FrmLogin";
            this.Resizable = false;
            this.Load += new System.EventHandler(this.FrmLogin_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private MetroFramework.Controls.MetroLabel lbluser;
        private MetroFramework.Controls.MetroLabel lblpass;
        private MetroFramework.Controls.MetroTextBox txtbuser;
        private MetroFramework.Controls.MetroTextBox txtbpas;
        private MetroFramework.Controls.MetroButton btnlog;
        private MetroFramework.Controls.MetroButton btnlogout;
        private System.Windows.Forms.Label lblesqas;
    }
}
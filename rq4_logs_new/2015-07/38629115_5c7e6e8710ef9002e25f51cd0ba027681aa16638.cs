namespace RoswellSignatureSync
{
    partial class ProjectInstaller
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

        #region Component Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.roswellSigSyncProcessInstaller1 = new System.ServiceProcess.ServiceProcessInstaller();
            this.roswellSigSyncInstaller1 = new System.ServiceProcess.ServiceInstaller();
            // 
            // roswellSigSyncProcessInstaller1
            // 
            this.roswellSigSyncProcessInstaller1.Account = System.ServiceProcess.ServiceAccount.LocalSystem;
            this.roswellSigSyncProcessInstaller1.Password = null;
            this.roswellSigSyncProcessInstaller1.Username = null;
            // 
            // roswellSigSyncInstaller1
            // 
            this.roswellSigSyncInstaller1.DelayedAutoStart = true;
            this.roswellSigSyncInstaller1.Description = "Syncs signatures across Outlook and office365 for an organisation from a central " +
    "location";
            this.roswellSigSyncInstaller1.DisplayName = "Roswell Signature Sync";
            this.roswellSigSyncInstaller1.ServiceName = "Roswell Signature Sync";
            this.roswellSigSyncInstaller1.StartType = System.ServiceProcess.ServiceStartMode.Automatic;
            // 
            // ProjectInstaller
            // 
            this.Installers.AddRange(new System.Configuration.Install.Installer[] {
            this.roswellSigSyncProcessInstaller1,
            this.roswellSigSyncInstaller1});

        }

        #endregion

        private System.ServiceProcess.ServiceProcessInstaller roswellSigSyncProcessInstaller1;
        private System.ServiceProcess.ServiceInstaller roswellSigSyncInstaller1;
    }
}
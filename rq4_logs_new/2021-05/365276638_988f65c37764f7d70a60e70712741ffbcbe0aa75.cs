
namespace BlightnautsDialogue
{
    partial class NewTriggerWindow
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
            System.Windows.Forms.Label labelTriggerName;
            System.Windows.Forms.Label labelTeamDialogues;
            this.textBoxTriggerName = new System.Windows.Forms.TextBox();
            this.numericUpDownTeamDialogues = new System.Windows.Forms.NumericUpDown();
            this.buttonOk = new System.Windows.Forms.Button();
            this.buttonCancel = new System.Windows.Forms.Button();
            labelTriggerName = new System.Windows.Forms.Label();
            labelTeamDialogues = new System.Windows.Forms.Label();
            ((System.ComponentModel.ISupportInitialize)(this.numericUpDownTeamDialogues)).BeginInit();
            this.SuspendLayout();
            // 
            // labelTriggerName
            // 
            labelTriggerName.AutoSize = true;
            labelTriggerName.Location = new System.Drawing.Point(9, 13);
            labelTriggerName.Name = "labelTriggerName";
            labelTriggerName.Size = new System.Drawing.Size(69, 13);
            labelTriggerName.TabIndex = 0;
            labelTriggerName.Text = "Trigger name";
            // 
            // labelTeamDialogues
            // 
            labelTeamDialogues.AutoSize = true;
            labelTeamDialogues.Location = new System.Drawing.Point(12, 56);
            labelTeamDialogues.Name = "labelTeamDialogues";
            labelTeamDialogues.Size = new System.Drawing.Size(82, 13);
            labelTeamDialogues.TabIndex = 2;
            labelTeamDialogues.Text = "Team dialogues";
            // 
            // textBoxTriggerName
            // 
            this.textBoxTriggerName.Location = new System.Drawing.Point(12, 29);
            this.textBoxTriggerName.MaxLength = 20;
            this.textBoxTriggerName.Name = "textBoxTriggerName";
            this.textBoxTriggerName.Size = new System.Drawing.Size(253, 20);
            this.textBoxTriggerName.TabIndex = 0;
            // 
            // numericUpDownTeamDialogues
            // 
            this.numericUpDownTeamDialogues.Location = new System.Drawing.Point(12, 73);
            this.numericUpDownTeamDialogues.Maximum = new decimal(new int[] {
            9,
            0,
            0,
            0});
            this.numericUpDownTeamDialogues.Minimum = new decimal(new int[] {
            1,
            0,
            0,
            0});
            this.numericUpDownTeamDialogues.Name = "numericUpDownTeamDialogues";
            this.numericUpDownTeamDialogues.Size = new System.Drawing.Size(35, 20);
            this.numericUpDownTeamDialogues.TabIndex = 1;
            this.numericUpDownTeamDialogues.TextAlign = System.Windows.Forms.HorizontalAlignment.Right;
            this.numericUpDownTeamDialogues.Value = new decimal(new int[] {
            1,
            0,
            0,
            0});
            // 
            // buttonOk
            // 
            this.buttonOk.DialogResult = System.Windows.Forms.DialogResult.OK;
            this.buttonOk.Location = new System.Drawing.Point(105, 70);
            this.buttonOk.Name = "buttonOk";
            this.buttonOk.Size = new System.Drawing.Size(75, 23);
            this.buttonOk.TabIndex = 2;
            this.buttonOk.Text = "Create";
            this.buttonOk.UseVisualStyleBackColor = true;
            this.buttonOk.Click += new System.EventHandler(this.buttonOk_Click);
            // 
            // buttonCancel
            // 
            this.buttonCancel.DialogResult = System.Windows.Forms.DialogResult.Cancel;
            this.buttonCancel.Location = new System.Drawing.Point(190, 70);
            this.buttonCancel.Name = "buttonCancel";
            this.buttonCancel.Size = new System.Drawing.Size(75, 23);
            this.buttonCancel.TabIndex = 3;
            this.buttonCancel.Text = "Cancel";
            this.buttonCancel.UseVisualStyleBackColor = true;
            this.buttonCancel.Click += new System.EventHandler(this.buttonCancel_Click);
            // 
            // NewTriggerWindow
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(277, 102);
            this.Controls.Add(this.buttonCancel);
            this.Controls.Add(this.buttonOk);
            this.Controls.Add(this.numericUpDownTeamDialogues);
            this.Controls.Add(labelTeamDialogues);
            this.Controls.Add(this.textBoxTriggerName);
            this.Controls.Add(labelTriggerName);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedToolWindow;
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "NewTriggerWindow";
            this.Text = "New Trigger";
            this.TopMost = true;
            ((System.ComponentModel.ISupportInitialize)(this.numericUpDownTeamDialogues)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox textBoxTriggerName;
        private System.Windows.Forms.NumericUpDown numericUpDownTeamDialogues;
        private System.Windows.Forms.Button buttonOk;
        private System.Windows.Forms.Button buttonCancel;
    }
}
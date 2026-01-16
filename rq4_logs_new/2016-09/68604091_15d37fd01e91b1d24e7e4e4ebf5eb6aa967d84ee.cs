namespace hippodrome
{
    partial class Fgeneral
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
            this.tabCourse = new System.Windows.Forms.TabPage();
            this.gbDetailCourse = new System.Windows.Forms.GroupBox();
            this.btSupprimerCourse = new System.Windows.Forms.Button();
            this.btModifierCourse = new System.Windows.Forms.Button();
            this.btAjoutCourse = new System.Windows.Forms.Button();
            this.label8 = new System.Windows.Forms.Label();
            this.label7 = new System.Windows.Forms.Label();
            this.label6 = new System.Windows.Forms.Label();
            this.label5 = new System.Windows.Forms.Label();
            this.tbDistance = new System.Windows.Forms.TextBox();
            this.tbDate = new System.Windows.Forms.TextBox();
            this.tbHippodrome = new System.Windows.Forms.TextBox();
            this.tbIdCourse = new System.Windows.Forms.TextBox();
            this.dgCourses = new System.Windows.Forms.DataGridView();
            this.tabgeneral = new System.Windows.Forms.TabControl();
            this.tabCheval = new System.Windows.Forms.TabPage();
            this.tabCourse.SuspendLayout();
            this.gbDetailCourse.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.dgCourses)).BeginInit();
            this.tabgeneral.SuspendLayout();
            this.SuspendLayout();
            // 
            // tabCourse
            // 
            this.tabCourse.Controls.Add(this.gbDetailCourse);
            this.tabCourse.Controls.Add(this.dgCourses);
            this.tabCourse.Font = new System.Drawing.Font("Calibri", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.tabCourse.Location = new System.Drawing.Point(4, 28);
            this.tabCourse.Name = "tabCourse";
            this.tabCourse.Padding = new System.Windows.Forms.Padding(3);
            this.tabCourse.Size = new System.Drawing.Size(1043, 472);
            this.tabCourse.TabIndex = 1;
            this.tabCourse.Text = "Les courses";
            this.tabCourse.UseVisualStyleBackColor = true;
            this.tabCourse.Enter += new System.EventHandler(this.tabCourse_Enter);
            // 
            // gbDetailCourse
            // 
            this.gbDetailCourse.Controls.Add(this.btSupprimerCourse);
            this.gbDetailCourse.Controls.Add(this.btModifierCourse);
            this.gbDetailCourse.Controls.Add(this.btAjoutCourse);
            this.gbDetailCourse.Controls.Add(this.label8);
            this.gbDetailCourse.Controls.Add(this.label7);
            this.gbDetailCourse.Controls.Add(this.label6);
            this.gbDetailCourse.Controls.Add(this.label5);
            this.gbDetailCourse.Controls.Add(this.tbDistance);
            this.gbDetailCourse.Controls.Add(this.tbDate);
            this.gbDetailCourse.Controls.Add(this.tbHippodrome);
            this.gbDetailCourse.Controls.Add(this.tbIdCourse);
            this.gbDetailCourse.Location = new System.Drawing.Point(606, 50);
            this.gbDetailCourse.Name = "gbDetailCourse";
            this.gbDetailCourse.Size = new System.Drawing.Size(313, 343);
            this.gbDetailCourse.TabIndex = 1;
            this.gbDetailCourse.TabStop = false;
            this.gbDetailCourse.Text = "Détail course";
            // 
            // btSupprimerCourse
            // 
            this.btSupprimerCourse.Location = new System.Drawing.Point(217, 274);
            this.btSupprimerCourse.Name = "btSupprimerCourse";
            this.btSupprimerCourse.Size = new System.Drawing.Size(96, 39);
            this.btSupprimerCourse.TabIndex = 10;
            this.btSupprimerCourse.Text = "&Supprimer";
            this.btSupprimerCourse.UseVisualStyleBackColor = true;
            // 
            // btModifierCourse
            // 
            this.btModifierCourse.Location = new System.Drawing.Point(113, 274);
            this.btModifierCourse.Name = "btModifierCourse";
            this.btModifierCourse.Size = new System.Drawing.Size(92, 38);
            this.btModifierCourse.TabIndex = 9;
            this.btModifierCourse.Text = "Modifier";
            this.btModifierCourse.UseVisualStyleBackColor = true;
            this.btModifierCourse.Click += new System.EventHandler(this.btModifierCourse_Click);
            // 
            // btAjoutCourse
            // 
            this.btAjoutCourse.Location = new System.Drawing.Point(11, 274);
            this.btAjoutCourse.Name = "btAjoutCourse";
            this.btAjoutCourse.Size = new System.Drawing.Size(96, 39);
            this.btAjoutCourse.TabIndex = 8;
            this.btAjoutCourse.Text = "&Ajouter";
            this.btAjoutCourse.UseVisualStyleBackColor = true;
            this.btAjoutCourse.Click += new System.EventHandler(this.btAjoutCourse_Click);
            // 
            // label8
            // 
            this.label8.AutoSize = true;
            this.label8.Location = new System.Drawing.Point(19, 91);
            this.label8.Name = "label8";
            this.label8.Size = new System.Drawing.Size(88, 19);
            this.label8.TabIndex = 7;
            this.label8.Text = "Hippodrome";
            // 
            // label7
            // 
            this.label7.AutoSize = true;
            this.label7.Location = new System.Drawing.Point(19, 157);
            this.label7.Name = "label7";
            this.label7.Size = new System.Drawing.Size(40, 19);
            this.label7.TabIndex = 6;
            this.label7.Text = "Date";
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Location = new System.Drawing.Point(19, 212);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(66, 19);
            this.label6.TabIndex = 5;
            this.label6.Text = "Distance";
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Location = new System.Drawing.Point(19, 37);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(68, 19);
            this.label5.TabIndex = 4;
            this.label5.Text = "id course";
            // 
            // tbDistance
            // 
            this.tbDistance.Location = new System.Drawing.Point(121, 204);
            this.tbDistance.Name = "tbDistance";
            this.tbDistance.Size = new System.Drawing.Size(108, 27);
            this.tbDistance.TabIndex = 3;
            // 
            // tbDate
            // 
            this.tbDate.Location = new System.Drawing.Point(121, 149);
            this.tbDate.Name = "tbDate";
            this.tbDate.Size = new System.Drawing.Size(108, 27);
            this.tbDate.TabIndex = 2;
            // 
            // tbHippodrome
            // 
            this.tbHippodrome.Location = new System.Drawing.Point(121, 91);
            this.tbHippodrome.Name = "tbHippodrome";
            this.tbHippodrome.Size = new System.Drawing.Size(108, 27);
            this.tbHippodrome.TabIndex = 1;
            // 
            // tbIdCourse
            // 
            this.tbIdCourse.Location = new System.Drawing.Point(121, 35);
            this.tbIdCourse.Name = "tbIdCourse";
            this.tbIdCourse.Size = new System.Drawing.Size(108, 27);
            this.tbIdCourse.TabIndex = 0;
            // 
            // dgCourses
            // 
            this.dgCourses.AllowUserToAddRows = false;
            this.dgCourses.AllowUserToDeleteRows = false;
            this.dgCourses.BackgroundColor = System.Drawing.SystemColors.Window;
            this.dgCourses.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dgCourses.Location = new System.Drawing.Point(30, 48);
            this.dgCourses.Name = "dgCourses";
            this.dgCourses.ReadOnly = true;
            this.dgCourses.Size = new System.Drawing.Size(548, 345);
            this.dgCourses.TabIndex = 0;
            this.dgCourses.RowEnter += new System.Windows.Forms.DataGridViewCellEventHandler(this.dgCourses_RowEnter);
            // 
            // tabgeneral
            // 
            this.tabgeneral.Controls.Add(this.tabCheval);
            this.tabgeneral.Controls.Add(this.tabCourse);
            this.tabgeneral.Font = new System.Drawing.Font("Calibri", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.tabgeneral.Location = new System.Drawing.Point(4, 5);
            this.tabgeneral.Name = "tabgeneral";
            this.tabgeneral.SelectedIndex = 0;
            this.tabgeneral.Size = new System.Drawing.Size(1051, 504);
            this.tabgeneral.TabIndex = 0;
            // 
            // tabCheval
            // 
            this.tabCheval.Font = new System.Drawing.Font("Calibri", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.tabCheval.Location = new System.Drawing.Point(4, 28);
            this.tabCheval.Name = "tabCheval";
            this.tabCheval.Padding = new System.Windows.Forms.Padding(3);
            this.tabCheval.Size = new System.Drawing.Size(1043, 472);
            this.tabCheval.TabIndex = 0;
            this.tabCheval.Text = "Les chevaux";
            this.tabCheval.UseVisualStyleBackColor = true;
            // 
            // Fgeneral
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1067, 511);
            this.Controls.Add(this.tabgeneral);
            this.Name = "Fgeneral";
            this.Text = "Fgeneral";
            this.Load += new System.EventHandler(this.Fgeneral_Load);
            this.tabCourse.ResumeLayout(false);
            this.gbDetailCourse.ResumeLayout(false);
            this.gbDetailCourse.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.dgCourses)).EndInit();
            this.tabgeneral.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.TabPage tabCourse;
        private System.Windows.Forms.GroupBox gbDetailCourse;
        private System.Windows.Forms.Button btSupprimerCourse;
        private System.Windows.Forms.Button btModifierCourse;
        private System.Windows.Forms.Button btAjoutCourse;
        private System.Windows.Forms.Label label8;
        private System.Windows.Forms.Label label7;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.TextBox tbDistance;
        private System.Windows.Forms.TextBox tbDate;
        private System.Windows.Forms.TextBox tbHippodrome;
        private System.Windows.Forms.TextBox tbIdCourse;
        private System.Windows.Forms.DataGridView dgCourses;
        private System.Windows.Forms.TabControl tabgeneral;
        private System.Windows.Forms.TabPage tabCheval;

    }
}
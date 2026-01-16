namespace ICT4Rails.Forms
{
    partial class TechnicianForm
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
            this.pl_Form_Total_Context = new System.Windows.Forms.Panel();
            this.pDefault = new System.Windows.Forms.Panel();
            this.panel3 = new System.Windows.Forms.Panel();
            this.label1 = new System.Windows.Forms.Label();
            this.pTramMaitenance = new System.Windows.Forms.Panel();
            this.panel1 = new System.Windows.Forms.Panel();
            this.btnLogOut = new System.Windows.Forms.Button();
            this.btnMaitenanceSchedule = new System.Windows.Forms.Button();
            this.btnTasks = new System.Windows.Forms.Button();
            this.btnTramMaitenance = new System.Windows.Forms.Button();
            this.btnWorkSchedule = new System.Windows.Forms.Button();
            this.dgvMaitenanceSchedule = new System.Windows.Forms.DataGridView();
            this.dataGridViewTextBoxColumn5 = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.dataGridViewTextBoxColumn6 = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.dataGridViewTextBoxColumn7 = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.dataGridViewTextBoxColumn9 = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.dataGridViewTextBoxColumn10 = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.lbListInfo = new System.Windows.Forms.Label();
            this.pTramInfo = new System.Windows.Forms.Panel();
            this.lblMaitenanceDescription = new System.Windows.Forms.Label();
            this.rtbMaitenanceDescription = new System.Windows.Forms.RichTextBox();
            this.lblTramStatus = new System.Windows.Forms.Label();
            this.cbTramStatus = new System.Windows.Forms.ComboBox();
            this.button2 = new System.Windows.Forms.Button();
            this.btnSubmitTram = new System.Windows.Forms.Button();
            this.lbTramInfo = new System.Windows.Forms.Label();
            this.label18 = new System.Windows.Forms.Label();
            this.tbTramLenght = new System.Windows.Forms.TextBox();
            this.label19 = new System.Windows.Forms.Label();
            this.tbTramType = new System.Windows.Forms.TextBox();
            this.label20 = new System.Windows.Forms.Label();
            this.tbTramID = new System.Windows.Forms.TextBox();
            this.btnTrams = new System.Windows.Forms.Button();
            this.dataGridViewTextBoxColumn3 = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.dataGridViewTextBoxColumn4 = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.dataGridViewTextBoxColumn2 = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.dataGridViewTextBoxColumn1 = new System.Windows.Forms.DataGridViewTextBoxColumn();
            this.dgvTrams = new System.Windows.Forms.DataGridView();
            this.label2 = new System.Windows.Forms.Label();
            this.dateTimePicker1 = new System.Windows.Forms.DateTimePicker();
            this.btnAddMaitenance = new System.Windows.Forms.Button();
            this.btnMaitenanceHistory = new System.Windows.Forms.Button();
            this.pl_Form_Total_Context.SuspendLayout();
            this.panel3.SuspendLayout();
            this.pTramMaitenance.SuspendLayout();
            this.panel1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.dgvMaitenanceSchedule)).BeginInit();
            this.pTramInfo.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.dgvTrams)).BeginInit();
            this.SuspendLayout();
            // 
            // pl_Form_Total_Context
            // 
            this.pl_Form_Total_Context.BackColor = System.Drawing.SystemColors.AppWorkspace;
            this.pl_Form_Total_Context.Controls.Add(this.pDefault);
            this.pl_Form_Total_Context.Controls.Add(this.panel3);
            this.pl_Form_Total_Context.Controls.Add(this.pTramMaitenance);
            this.pl_Form_Total_Context.Controls.Add(this.panel1);
            this.pl_Form_Total_Context.Location = new System.Drawing.Point(12, 12);
            this.pl_Form_Total_Context.Name = "pl_Form_Total_Context";
            this.pl_Form_Total_Context.Size = new System.Drawing.Size(1326, 705);
            this.pl_Form_Total_Context.TabIndex = 1;
            // 
            // pDefault
            // 
            this.pDefault.BackColor = System.Drawing.SystemColors.ButtonHighlight;
            this.pDefault.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.pDefault.Location = new System.Drawing.Point(226, 668);
            this.pDefault.Name = "pDefault";
            this.pDefault.Size = new System.Drawing.Size(1097, 34);
            this.pDefault.TabIndex = 20;
            // 
            // panel3
            // 
            this.panel3.BackColor = System.Drawing.SystemColors.Control;
            this.panel3.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.panel3.Controls.Add(this.label1);
            this.panel3.Location = new System.Drawing.Point(3, 3);
            this.panel3.Name = "panel3";
            this.panel3.Size = new System.Drawing.Size(1320, 81);
            this.panel3.TabIndex = 2;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 36F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.Location = new System.Drawing.Point(499, 13);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(315, 55);
            this.label1.TabIndex = 0;
            this.label1.Text = "EyeCT4Rails";
            // 
            // pTramMaitenance
            // 
            this.pTramMaitenance.BackColor = System.Drawing.SystemColors.ButtonHighlight;
            this.pTramMaitenance.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.pTramMaitenance.Controls.Add(this.btnAddMaitenance);
            this.pTramMaitenance.Controls.Add(this.btnMaitenanceHistory);
            this.pTramMaitenance.Controls.Add(this.pTramInfo);
            this.pTramMaitenance.Controls.Add(this.lbListInfo);
            this.pTramMaitenance.Controls.Add(this.dgvMaitenanceSchedule);
            this.pTramMaitenance.Controls.Add(this.dgvTrams);
            this.pTramMaitenance.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F);
            this.pTramMaitenance.Location = new System.Drawing.Point(226, 90);
            this.pTramMaitenance.Name = "pTramMaitenance";
            this.pTramMaitenance.Size = new System.Drawing.Size(1097, 612);
            this.pTramMaitenance.TabIndex = 1;
            this.pTramMaitenance.Visible = false;
            // 
            // panel1
            // 
            this.panel1.BackColor = System.Drawing.SystemColors.ButtonHighlight;
            this.panel1.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.panel1.Controls.Add(this.btnLogOut);
            this.panel1.Controls.Add(this.btnMaitenanceSchedule);
            this.panel1.Controls.Add(this.btnTrams);
            this.panel1.Controls.Add(this.btnTasks);
            this.panel1.Controls.Add(this.btnTramMaitenance);
            this.panel1.Controls.Add(this.btnWorkSchedule);
            this.panel1.Location = new System.Drawing.Point(3, 90);
            this.panel1.Name = "panel1";
            this.panel1.Size = new System.Drawing.Size(217, 612);
            this.panel1.TabIndex = 0;
            // 
            // btnLogOut
            // 
            this.btnLogOut.BackColor = System.Drawing.Color.DodgerBlue;
            this.btnLogOut.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btnLogOut.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnLogOut.Location = new System.Drawing.Point(3, 577);
            this.btnLogOut.Name = "btnLogOut";
            this.btnLogOut.Size = new System.Drawing.Size(209, 30);
            this.btnLogOut.TabIndex = 18;
            this.btnLogOut.Text = "Log Out";
            this.btnLogOut.UseVisualStyleBackColor = false;
            this.btnLogOut.Click += new System.EventHandler(this.btnLogOut_Click);
            // 
            // btnMaitenanceSchedule
            // 
            this.btnMaitenanceSchedule.BackColor = System.Drawing.Color.White;
            this.btnMaitenanceSchedule.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btnMaitenanceSchedule.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnMaitenanceSchedule.Location = new System.Drawing.Point(3, 131);
            this.btnMaitenanceSchedule.Name = "btnMaitenanceSchedule";
            this.btnMaitenanceSchedule.Size = new System.Drawing.Size(209, 30);
            this.btnMaitenanceSchedule.TabIndex = 7;
            this.btnMaitenanceSchedule.Text = "Maitenance Schedule";
            this.btnMaitenanceSchedule.UseVisualStyleBackColor = false;
            this.btnMaitenanceSchedule.Click += new System.EventHandler(this.btnMaitenanceSchedule_Click);
            // 
            // btnTasks
            // 
            this.btnTasks.BackColor = System.Drawing.Color.White;
            this.btnTasks.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btnTasks.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnTasks.Location = new System.Drawing.Point(3, 35);
            this.btnTasks.Name = "btnTasks";
            this.btnTasks.Size = new System.Drawing.Size(209, 30);
            this.btnTasks.TabIndex = 3;
            this.btnTasks.Text = "Tasks";
            this.btnTasks.UseVisualStyleBackColor = false;
            // 
            // btnTramMaitenance
            // 
            this.btnTramMaitenance.BackColor = System.Drawing.Color.DodgerBlue;
            this.btnTramMaitenance.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btnTramMaitenance.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnTramMaitenance.Location = new System.Drawing.Point(3, 67);
            this.btnTramMaitenance.Name = "btnTramMaitenance";
            this.btnTramMaitenance.Size = new System.Drawing.Size(209, 30);
            this.btnTramMaitenance.TabIndex = 1;
            this.btnTramMaitenance.Text = "Tram Maitenance";
            this.btnTramMaitenance.UseVisualStyleBackColor = false;
            this.btnTramMaitenance.Click += new System.EventHandler(this.btnTramMaitenance_Click);
            // 
            // btnWorkSchedule
            // 
            this.btnWorkSchedule.BackColor = System.Drawing.Color.DodgerBlue;
            this.btnWorkSchedule.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btnWorkSchedule.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnWorkSchedule.Location = new System.Drawing.Point(3, 3);
            this.btnWorkSchedule.Name = "btnWorkSchedule";
            this.btnWorkSchedule.Size = new System.Drawing.Size(209, 30);
            this.btnWorkSchedule.TabIndex = 0;
            this.btnWorkSchedule.Text = "Work Schedule";
            this.btnWorkSchedule.UseVisualStyleBackColor = false;
            this.btnWorkSchedule.Click += new System.EventHandler(this.btnWorkSchedule_Click);
            // 
            // dgvMaitenanceSchedule
            // 
            this.dgvMaitenanceSchedule.AutoSizeColumnsMode = System.Windows.Forms.DataGridViewAutoSizeColumnsMode.Fill;
            this.dgvMaitenanceSchedule.BackgroundColor = System.Drawing.Color.Gainsboro;
            this.dgvMaitenanceSchedule.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dgvMaitenanceSchedule.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.dataGridViewTextBoxColumn5,
            this.dataGridViewTextBoxColumn6,
            this.dataGridViewTextBoxColumn7,
            this.dataGridViewTextBoxColumn9,
            this.dataGridViewTextBoxColumn10});
            this.dgvMaitenanceSchedule.Location = new System.Drawing.Point(16, 50);
            this.dgvMaitenanceSchedule.Name = "dgvMaitenanceSchedule";
            this.dgvMaitenanceSchedule.Size = new System.Drawing.Size(518, 541);
            this.dgvMaitenanceSchedule.TabIndex = 33;
            this.dgvMaitenanceSchedule.TabStop = false;
            this.dgvMaitenanceSchedule.Visible = false;
            // 
            // dataGridViewTextBoxColumn5
            // 
            this.dataGridViewTextBoxColumn5.FillWeight = 1F;
            this.dataGridViewTextBoxColumn5.HeaderText = "Tram ID";
            this.dataGridViewTextBoxColumn5.Name = "dataGridViewTextBoxColumn5";
            this.dataGridViewTextBoxColumn5.ReadOnly = true;
            // 
            // dataGridViewTextBoxColumn6
            // 
            this.dataGridViewTextBoxColumn6.FillWeight = 1F;
            this.dataGridViewTextBoxColumn6.HeaderText = "MaintenanceType";
            this.dataGridViewTextBoxColumn6.Name = "dataGridViewTextBoxColumn6";
            // 
            // dataGridViewTextBoxColumn7
            // 
            this.dataGridViewTextBoxColumn7.FillWeight = 2F;
            this.dataGridViewTextBoxColumn7.HeaderText = "Tram Status";
            this.dataGridViewTextBoxColumn7.Name = "dataGridViewTextBoxColumn7";
            // 
            // dataGridViewTextBoxColumn9
            // 
            this.dataGridViewTextBoxColumn9.FillWeight = 1F;
            this.dataGridViewTextBoxColumn9.HeaderText = "Start Time";
            this.dataGridViewTextBoxColumn9.Name = "dataGridViewTextBoxColumn9";
            // 
            // dataGridViewTextBoxColumn10
            // 
            this.dataGridViewTextBoxColumn10.FillWeight = 1F;
            this.dataGridViewTextBoxColumn10.HeaderText = "End Time";
            this.dataGridViewTextBoxColumn10.Name = "dataGridViewTextBoxColumn10";
            // 
            // lbListInfo
            // 
            this.lbListInfo.AutoSize = true;
            this.lbListInfo.Font = new System.Drawing.Font("Microsoft Sans Serif", 20.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lbListInfo.Location = new System.Drawing.Point(173, 16);
            this.lbListInfo.Name = "lbListInfo";
            this.lbListInfo.Size = new System.Drawing.Size(184, 31);
            this.lbListInfo.TabIndex = 2;
            this.lbListInfo.Text = "List of Trams";
            // 
            // pTramInfo
            // 
            this.pTramInfo.BackColor = System.Drawing.Color.Gainsboro;
            this.pTramInfo.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.pTramInfo.Controls.Add(this.dateTimePicker1);
            this.pTramInfo.Controls.Add(this.label2);
            this.pTramInfo.Controls.Add(this.lblMaitenanceDescription);
            this.pTramInfo.Controls.Add(this.rtbMaitenanceDescription);
            this.pTramInfo.Controls.Add(this.lblTramStatus);
            this.pTramInfo.Controls.Add(this.cbTramStatus);
            this.pTramInfo.Controls.Add(this.button2);
            this.pTramInfo.Controls.Add(this.btnSubmitTram);
            this.pTramInfo.Controls.Add(this.lbTramInfo);
            this.pTramInfo.Controls.Add(this.label18);
            this.pTramInfo.Controls.Add(this.tbTramLenght);
            this.pTramInfo.Controls.Add(this.label19);
            this.pTramInfo.Controls.Add(this.tbTramType);
            this.pTramInfo.Controls.Add(this.label20);
            this.pTramInfo.Controls.Add(this.tbTramID);
            this.pTramInfo.Location = new System.Drawing.Point(549, 50);
            this.pTramInfo.Name = "pTramInfo";
            this.pTramInfo.Size = new System.Drawing.Size(530, 505);
            this.pTramInfo.TabIndex = 35;
            this.pTramInfo.Visible = false;
            // 
            // lblMaitenanceDescription
            // 
            this.lblMaitenanceDescription.AutoSize = true;
            this.lblMaitenanceDescription.Location = new System.Drawing.Point(44, 282);
            this.lblMaitenanceDescription.Name = "lblMaitenanceDescription";
            this.lblMaitenanceDescription.Size = new System.Drawing.Size(152, 16);
            this.lblMaitenanceDescription.TabIndex = 27;
            this.lblMaitenanceDescription.Text = "Maitenance Description:";
            // 
            // rtbMaitenanceDescription
            // 
            this.rtbMaitenanceDescription.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.rtbMaitenanceDescription.Location = new System.Drawing.Point(47, 299);
            this.rtbMaitenanceDescription.Name = "rtbMaitenanceDescription";
            this.rtbMaitenanceDescription.Size = new System.Drawing.Size(428, 155);
            this.rtbMaitenanceDescription.TabIndex = 26;
            this.rtbMaitenanceDescription.Text = "";
            // 
            // lblTramStatus
            // 
            this.lblTramStatus.AutoSize = true;
            this.lblTramStatus.Location = new System.Drawing.Point(44, 227);
            this.lblTramStatus.Name = "lblTramStatus";
            this.lblTramStatus.Size = new System.Drawing.Size(83, 16);
            this.lblTramStatus.TabIndex = 25;
            this.lblTramStatus.Text = "Tram Status:";
            // 
            // cbTramStatus
            // 
            this.cbTramStatus.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.cbTramStatus.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.cbTramStatus.FormattingEnabled = true;
            this.cbTramStatus.Items.AddRange(new object[] {
            "Cleaning required",
            "Reparation required",
            "Ready for use"});
            this.cbTramStatus.Location = new System.Drawing.Point(47, 244);
            this.cbTramStatus.Name = "cbTramStatus";
            this.cbTramStatus.Size = new System.Drawing.Size(121, 24);
            this.cbTramStatus.TabIndex = 24;
            // 
            // button2
            // 
            this.button2.BackColor = System.Drawing.Color.White;
            this.button2.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.button2.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.button2.Location = new System.Drawing.Point(141, 460);
            this.button2.Name = "button2";
            this.button2.Size = new System.Drawing.Size(88, 25);
            this.button2.TabIndex = 22;
            this.button2.Text = "Cancel";
            this.button2.UseVisualStyleBackColor = false;
            // 
            // btnSubmitTram
            // 
            this.btnSubmitTram.BackColor = System.Drawing.Color.White;
            this.btnSubmitTram.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btnSubmitTram.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnSubmitTram.Location = new System.Drawing.Point(47, 460);
            this.btnSubmitTram.Name = "btnSubmitTram";
            this.btnSubmitTram.Size = new System.Drawing.Size(88, 25);
            this.btnSubmitTram.TabIndex = 19;
            this.btnSubmitTram.Text = "Submit";
            this.btnSubmitTram.UseVisualStyleBackColor = false;
            // 
            // lbTramInfo
            // 
            this.lbTramInfo.AutoSize = true;
            this.lbTramInfo.Font = new System.Drawing.Font("Microsoft Sans Serif", 15.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lbTramInfo.Location = new System.Drawing.Point(42, 19);
            this.lbTramInfo.Name = "lbTramInfo";
            this.lbTramInfo.Size = new System.Drawing.Size(180, 25);
            this.lbTramInfo.TabIndex = 19;
            this.lbTramInfo.Text = "Maitenance Info";
            // 
            // label18
            // 
            this.label18.AutoSize = true;
            this.label18.Location = new System.Drawing.Point(44, 172);
            this.label18.Name = "label18";
            this.label18.Size = new System.Drawing.Size(86, 16);
            this.label18.TabIndex = 5;
            this.label18.Text = "Tram Lenght:";
            // 
            // tbTramLenght
            // 
            this.tbTramLenght.Location = new System.Drawing.Point(47, 188);
            this.tbTramLenght.Name = "tbTramLenght";
            this.tbTramLenght.Size = new System.Drawing.Size(428, 22);
            this.tbTramLenght.TabIndex = 4;
            // 
            // label19
            // 
            this.label19.AutoSize = true;
            this.label19.Location = new System.Drawing.Point(44, 116);
            this.label19.Name = "label19";
            this.label19.Size = new System.Drawing.Size(78, 16);
            this.label19.TabIndex = 3;
            this.label19.Text = "Tram Type:";
            // 
            // tbTramType
            // 
            this.tbTramType.Location = new System.Drawing.Point(47, 132);
            this.tbTramType.Name = "tbTramType";
            this.tbTramType.Size = new System.Drawing.Size(428, 22);
            this.tbTramType.TabIndex = 2;
            // 
            // label20
            // 
            this.label20.AutoSize = true;
            this.label20.Location = new System.Drawing.Point(44, 60);
            this.label20.Name = "label20";
            this.label20.Size = new System.Drawing.Size(59, 16);
            this.label20.TabIndex = 1;
            this.label20.Text = "Tram ID:";
            // 
            // tbTramID
            // 
            this.tbTramID.Location = new System.Drawing.Point(47, 76);
            this.tbTramID.Name = "tbTramID";
            this.tbTramID.Size = new System.Drawing.Size(428, 22);
            this.tbTramID.TabIndex = 0;
            // 
            // btnTrams
            // 
            this.btnTrams.BackColor = System.Drawing.Color.White;
            this.btnTrams.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btnTrams.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnTrams.Location = new System.Drawing.Point(3, 99);
            this.btnTrams.Name = "btnTrams";
            this.btnTrams.Size = new System.Drawing.Size(209, 30);
            this.btnTrams.TabIndex = 6;
            this.btnTrams.Text = "Trams";
            this.btnTrams.UseVisualStyleBackColor = false;
            this.btnTrams.Click += new System.EventHandler(this.btnTrams_Click);
            // 
            // dataGridViewTextBoxColumn3
            // 
            this.dataGridViewTextBoxColumn3.FillWeight = 1F;
            this.dataGridViewTextBoxColumn3.HeaderText = "Tram Lenght";
            this.dataGridViewTextBoxColumn3.Name = "dataGridViewTextBoxColumn3";
            this.dataGridViewTextBoxColumn3.ReadOnly = true;
            // 
            // dataGridViewTextBoxColumn4
            // 
            this.dataGridViewTextBoxColumn4.FillWeight = 1F;
            this.dataGridViewTextBoxColumn4.HeaderText = "Tram Status";
            this.dataGridViewTextBoxColumn4.Name = "dataGridViewTextBoxColumn4";
            // 
            // dataGridViewTextBoxColumn2
            // 
            this.dataGridViewTextBoxColumn2.FillWeight = 1F;
            this.dataGridViewTextBoxColumn2.HeaderText = "Tram Type";
            this.dataGridViewTextBoxColumn2.Name = "dataGridViewTextBoxColumn2";
            this.dataGridViewTextBoxColumn2.ReadOnly = true;
            // 
            // dataGridViewTextBoxColumn1
            // 
            this.dataGridViewTextBoxColumn1.FillWeight = 1F;
            this.dataGridViewTextBoxColumn1.HeaderText = "Tram ID";
            this.dataGridViewTextBoxColumn1.Name = "dataGridViewTextBoxColumn1";
            this.dataGridViewTextBoxColumn1.ReadOnly = true;
            // 
            // dgvTrams
            // 
            this.dgvTrams.AutoSizeColumnsMode = System.Windows.Forms.DataGridViewAutoSizeColumnsMode.Fill;
            this.dgvTrams.BackgroundColor = System.Drawing.Color.Gainsboro;
            this.dgvTrams.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dgvTrams.Columns.AddRange(new System.Windows.Forms.DataGridViewColumn[] {
            this.dataGridViewTextBoxColumn1,
            this.dataGridViewTextBoxColumn2,
            this.dataGridViewTextBoxColumn4,
            this.dataGridViewTextBoxColumn3});
            this.dgvTrams.Location = new System.Drawing.Point(16, 50);
            this.dgvTrams.Name = "dgvTrams";
            this.dgvTrams.Size = new System.Drawing.Size(518, 541);
            this.dgvTrams.TabIndex = 34;
            this.dgvTrams.TabStop = false;
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(174, 227);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(134, 16);
            this.label2.TabIndex = 29;
            this.label2.Text = "Maitenance Duration:";
            // 
            // dateTimePicker1
            // 
            this.dateTimePicker1.Format = System.Windows.Forms.DateTimePickerFormat.Time;
            this.dateTimePicker1.Location = new System.Drawing.Point(177, 245);
            this.dateTimePicker1.Name = "dateTimePicker1";
            this.dateTimePicker1.Size = new System.Drawing.Size(131, 22);
            this.dateTimePicker1.TabIndex = 30;
            // 
            // btnAddMaitenance
            // 
            this.btnAddMaitenance.BackColor = System.Drawing.Color.White;
            this.btnAddMaitenance.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btnAddMaitenance.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnAddMaitenance.Location = new System.Drawing.Point(549, 561);
            this.btnAddMaitenance.Name = "btnAddMaitenance";
            this.btnAddMaitenance.Size = new System.Drawing.Size(126, 30);
            this.btnAddMaitenance.TabIndex = 37;
            this.btnAddMaitenance.Text = "Add Maitenance";
            this.btnAddMaitenance.UseVisualStyleBackColor = false;
            this.btnAddMaitenance.Click += new System.EventHandler(this.btnAddMaitenance_Click);
            // 
            // btnMaitenanceHistory
            // 
            this.btnMaitenanceHistory.BackColor = System.Drawing.Color.White;
            this.btnMaitenanceHistory.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btnMaitenanceHistory.Font = new System.Drawing.Font("Microsoft Sans Serif", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnMaitenanceHistory.Location = new System.Drawing.Point(681, 561);
            this.btnMaitenanceHistory.Name = "btnMaitenanceHistory";
            this.btnMaitenanceHistory.Size = new System.Drawing.Size(135, 30);
            this.btnMaitenanceHistory.TabIndex = 36;
            this.btnMaitenanceHistory.Text = "Maitenance History";
            this.btnMaitenanceHistory.UseVisualStyleBackColor = false;
            // 
            // TechnicianForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1350, 729);
            this.Controls.Add(this.pl_Form_Total_Context);
            this.Name = "TechnicianForm";
            this.Text = "TechnicianForm";
            this.WindowState = System.Windows.Forms.FormWindowState.Maximized;
            this.Resize += new System.EventHandler(this.TechnicianForm_Resize);
            this.pl_Form_Total_Context.ResumeLayout(false);
            this.panel3.ResumeLayout(false);
            this.panel3.PerformLayout();
            this.pTramMaitenance.ResumeLayout(false);
            this.pTramMaitenance.PerformLayout();
            this.panel1.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.dgvMaitenanceSchedule)).EndInit();
            this.pTramInfo.ResumeLayout(false);
            this.pTramInfo.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.dgvTrams)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Panel pl_Form_Total_Context;
        private System.Windows.Forms.Panel pDefault;
        private System.Windows.Forms.Panel panel3;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Panel pTramMaitenance;
        private System.Windows.Forms.Panel panel1;
        private System.Windows.Forms.Button btnLogOut;
        private System.Windows.Forms.Button btnMaitenanceSchedule;
        private System.Windows.Forms.Button btnTasks;
        private System.Windows.Forms.Button btnTramMaitenance;
        private System.Windows.Forms.Button btnWorkSchedule;
        private System.Windows.Forms.DataGridView dgvMaitenanceSchedule;
        private System.Windows.Forms.DataGridViewTextBoxColumn dataGridViewTextBoxColumn5;
        private System.Windows.Forms.DataGridViewTextBoxColumn dataGridViewTextBoxColumn6;
        private System.Windows.Forms.DataGridViewTextBoxColumn dataGridViewTextBoxColumn7;
        private System.Windows.Forms.DataGridViewTextBoxColumn dataGridViewTextBoxColumn9;
        private System.Windows.Forms.DataGridViewTextBoxColumn dataGridViewTextBoxColumn10;
        private System.Windows.Forms.Label lbListInfo;
        private System.Windows.Forms.Panel pTramInfo;
        private System.Windows.Forms.Label lblMaitenanceDescription;
        private System.Windows.Forms.RichTextBox rtbMaitenanceDescription;
        private System.Windows.Forms.Label lblTramStatus;
        private System.Windows.Forms.ComboBox cbTramStatus;
        private System.Windows.Forms.Button button2;
        private System.Windows.Forms.Button btnSubmitTram;
        private System.Windows.Forms.Label lbTramInfo;
        private System.Windows.Forms.Label label18;
        private System.Windows.Forms.TextBox tbTramLenght;
        private System.Windows.Forms.Label label19;
        private System.Windows.Forms.TextBox tbTramType;
        private System.Windows.Forms.Label label20;
        private System.Windows.Forms.TextBox tbTramID;
        private System.Windows.Forms.DateTimePicker dateTimePicker1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.DataGridView dgvTrams;
        private System.Windows.Forms.DataGridViewTextBoxColumn dataGridViewTextBoxColumn1;
        private System.Windows.Forms.DataGridViewTextBoxColumn dataGridViewTextBoxColumn2;
        private System.Windows.Forms.DataGridViewTextBoxColumn dataGridViewTextBoxColumn4;
        private System.Windows.Forms.DataGridViewTextBoxColumn dataGridViewTextBoxColumn3;
        private System.Windows.Forms.Button btnTrams;
        private System.Windows.Forms.Button btnAddMaitenance;
        private System.Windows.Forms.Button btnMaitenanceHistory;
    }
}
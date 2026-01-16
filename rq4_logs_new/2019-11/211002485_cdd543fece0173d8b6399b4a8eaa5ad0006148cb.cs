            userControls = new UserControl[4] { stockList1, clientRegisterU1, order1,clientList21 };

        private void btn_ClientList_Click(object sender, EventArgs e)
        {
            pc.Set((Button)sender, selectPanel);
            pc.Chenge(userControls, clientList21);
        }
            this.dataGridView1 = new System.Windows.Forms.DataGridView();
            this.panel2 = new System.Windows.Forms.Panel();
            this.lbl_birthday = new System.Windows.Forms.Label();
            this.dtp_birthday = new System.Windows.Forms.DateTimePicker();
            this.panel3 = new System.Windows.Forms.Panel();
            this.panel1 = new System.Windows.Forms.Panel();
            this.panel4 = new System.Windows.Forms.Panel();
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.btn_search = new System.Windows.Forms.Button();
            this.panel2.SuspendLayout();
            this.groupBox1.SuspendLayout();
            // dataGridView1
            // 
            this.dataGridView1.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dataGridView1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.dataGridView1.Location = new System.Drawing.Point(0, 0);
            this.dataGridView1.Margin = new System.Windows.Forms.Padding(2, 2, 2, 2);
            this.dataGridView1.Name = "dataGridView1";
            this.dataGridView1.RowTemplate.Height = 24;
            this.dataGridView1.Size = new System.Drawing.Size(900, 448);
            this.dataGridView1.TabIndex = 40;
            // 
            // panel2
            // 
            this.panel2.Controls.Add(this.groupBox1);
            this.panel2.Controls.Add(this.lbl_birthday);
            this.panel2.Controls.Add(this.dtp_birthday);
            this.panel2.Dock = System.Windows.Forms.DockStyle.Top;
            this.panel2.Location = new System.Drawing.Point(0, 0);
            this.panel2.Margin = new System.Windows.Forms.Padding(2, 2, 2, 2);
            this.panel2.Name = "panel2";
            this.panel2.Size = new System.Drawing.Size(900, 113);
            this.panel2.TabIndex = 43;
            // 
            // lbl_birthday
            // 
            this.lbl_birthday.AutoSize = true;
            this.lbl_birthday.Font = new System.Drawing.Font("MS UI Gothic", 18F);
            this.lbl_birthday.Location = new System.Drawing.Point(509, 18);
            this.lbl_birthday.Name = "lbl_birthday";
            this.lbl_birthday.Size = new System.Drawing.Size(111, 24);
            this.lbl_birthday.TabIndex = 46;
            this.lbl_birthday.Text = "生年月日:";
            // 
            // dtp_birthday
            // 
            this.dtp_birthday.CalendarFont = new System.Drawing.Font("MS UI Gothic", 24F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(128)));
            this.dtp_birthday.Font = new System.Drawing.Font("MS UI Gothic", 18F);
            this.dtp_birthday.Location = new System.Drawing.Point(626, 13);
            this.dtp_birthday.Name = "dtp_birthday";
            this.dtp_birthday.Size = new System.Drawing.Size(218, 31);
            this.dtp_birthday.TabIndex = 45;
            // 
            // panel3
            // 
            this.panel3.Dock = System.Windows.Forms.DockStyle.Right;
            this.panel3.Location = new System.Drawing.Point(892, 113);
            this.panel3.Margin = new System.Windows.Forms.Padding(2, 2, 2, 2);
            this.panel3.Name = "panel3";
            this.panel3.Size = new System.Drawing.Size(8, 335);
            this.panel3.TabIndex = 44;
            // 
            // panel1
            // 
            this.panel1.Dock = System.Windows.Forms.DockStyle.Left;
            this.panel1.Location = new System.Drawing.Point(0, 113);
            this.panel1.Margin = new System.Windows.Forms.Padding(2, 2, 2, 2);
            this.panel1.Name = "panel1";
            this.panel1.Size = new System.Drawing.Size(8, 335);
            this.panel1.TabIndex = 45;
            // 
            // panel4
            // 
            this.panel4.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.panel4.Location = new System.Drawing.Point(8, 438);
            this.panel4.Margin = new System.Windows.Forms.Padding(2, 2, 2, 2);
            this.panel4.Name = "panel4";
            this.panel4.Size = new System.Drawing.Size(884, 10);
            this.panel4.TabIndex = 46;
            // 
            // groupBox1
            // 
            this.groupBox1.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.groupBox1.Controls.Add(this.btn_search);
            this.groupBox1.Controls.Add(this.cmb_sex);
            this.groupBox1.Controls.Add(this.txt_pos);
            this.groupBox1.Controls.Add(this.lbl_pos);
            this.groupBox1.Controls.Add(this.txt_id);
            this.groupBox1.Controls.Add(this.lbl_id);
            this.groupBox1.Controls.Add(this.txt_tel);
            this.groupBox1.Controls.Add(this.lbl_tel);
            this.groupBox1.Controls.Add(this.lbl_address);
            this.groupBox1.Controls.Add(this.txt_address);
            this.groupBox1.Controls.Add(this.lbl_name);
            this.groupBox1.Controls.Add(this.lbl_sex);
            this.groupBox1.Controls.Add(this.txt_name);
            this.groupBox1.Location = new System.Drawing.Point(68, -1);
            this.groupBox1.Margin = new System.Windows.Forms.Padding(2);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Padding = new System.Windows.Forms.Padding(2);
            this.groupBox1.Size = new System.Drawing.Size(764, 115);
            this.groupBox1.TabIndex = 48;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "絞り込み";
            // 
            // btn_search
            // 
            this.btn_search.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.btn_search.BackColor = System.Drawing.Color.Transparent;
            this.btn_search.FlatStyle = System.Windows.Forms.FlatStyle.Flat;
            this.btn_search.Font = new System.Drawing.Font("MS UI Gothic", 13.8F);
            this.btn_search.Location = new System.Drawing.Point(641, 29);
            this.btn_search.Margin = new System.Windows.Forms.Padding(2);
            this.btn_search.Name = "btn_search";
            this.btn_search.Size = new System.Drawing.Size(110, 54);
            this.btn_search.TabIndex = 69;
            this.btn_search.Text = "検索";
            this.btn_search.UseVisualStyleBackColor = false;
            // 
            this.cmb_sex.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this.cmb_sex.Font = new System.Drawing.Font("MS UI Gothic", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(128)));
            this.cmb_sex.Location = new System.Drawing.Point(665, 17);
            this.cmb_sex.Margin = new System.Windows.Forms.Padding(2);
            this.cmb_sex.Size = new System.Drawing.Size(161, 24);
            this.cmb_sex.TabIndex = 68;
            this.txt_pos.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this.txt_pos.Font = new System.Drawing.Font("MS UI Gothic", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(128)));
            this.txt_pos.Location = new System.Drawing.Point(76, 61);
            this.txt_pos.Size = new System.Drawing.Size(218, 23);
            this.txt_pos.TabIndex = 67;
            this.lbl_pos.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this.lbl_pos.Font = new System.Drawing.Font("MS UI Gothic", 13.8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(128)));
            this.lbl_pos.Location = new System.Drawing.Point(-15, 60);
            this.lbl_pos.Size = new System.Drawing.Size(95, 19);
            this.lbl_pos.TabIndex = 66;
            this.txt_id.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this.txt_id.Font = new System.Drawing.Font("MS UI Gothic", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(128)));
            this.txt_id.Location = new System.Drawing.Point(369, 16);
            this.txt_id.Size = new System.Drawing.Size(218, 23);
            this.txt_id.TabIndex = 65;
            this.lbl_id.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this.lbl_id.Font = new System.Drawing.Font("MS UI Gothic", 13.8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(128)));
            this.lbl_id.Location = new System.Drawing.Point(4, 14);
            this.lbl_id.Margin = new System.Windows.Forms.Padding(2, 0, 2, 0);
            this.lbl_id.Size = new System.Drawing.Size(74, 19);
            this.lbl_id.TabIndex = 64;
            this.txt_tel.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this.txt_tel.Font = new System.Drawing.Font("MS UI Gothic", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(128)));
            this.txt_tel.Location = new System.Drawing.Point(661, 66);
            this.txt_tel.Size = new System.Drawing.Size(165, 23);
            this.txt_tel.TabIndex = 63;
            this.lbl_tel.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this.lbl_tel.Font = new System.Drawing.Font("MS UI Gothic", 13.8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(128)));
            this.lbl_tel.Location = new System.Drawing.Point(613, 66);
            this.lbl_tel.Size = new System.Drawing.Size(45, 19);
            this.lbl_tel.TabIndex = 62;
            this.lbl_address.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this.lbl_address.Font = new System.Drawing.Font("MS UI Gothic", 13.8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(128)));
            this.lbl_address.Location = new System.Drawing.Point(316, 61);
            this.lbl_address.Size = new System.Drawing.Size(51, 19);
            this.lbl_address.TabIndex = 61;
            this.txt_address.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this.txt_address.Font = new System.Drawing.Font("MS UI Gothic", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(128)));
            this.txt_address.Location = new System.Drawing.Point(369, 60);
            this.txt_address.Size = new System.Drawing.Size(218, 23);
            this.txt_address.TabIndex = 60;
            this.lbl_name.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this.lbl_name.Font = new System.Drawing.Font("MS UI Gothic", 13.8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(128)));
            this.lbl_name.Location = new System.Drawing.Point(316, 14);
            this.lbl_name.Size = new System.Drawing.Size(51, 19);
            this.lbl_name.TabIndex = 58;
            this.lbl_sex.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this.lbl_sex.Font = new System.Drawing.Font("MS UI Gothic", 13.8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(128)));
            this.lbl_sex.Location = new System.Drawing.Point(613, 17);
            this.lbl_sex.Size = new System.Drawing.Size(51, 19);
            this.lbl_sex.TabIndex = 59;
            this.txt_name.Anchor = System.Windows.Forms.AnchorStyles.Left;
            this.txt_name.Font = new System.Drawing.Font("MS UI Gothic", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(128)));
            this.txt_name.Location = new System.Drawing.Point(76, 14);
            this.txt_name.Size = new System.Drawing.Size(218, 23);
            this.txt_name.TabIndex = 57;
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 12F);
            this.Controls.Add(this.panel4);
            this.Controls.Add(this.panel1);
            this.Controls.Add(this.panel3);
            this.Controls.Add(this.panel2);
            this.Margin = new System.Windows.Forms.Padding(2, 2, 2, 2);
            this.Size = new System.Drawing.Size(900, 448);
            this.panel2.ResumeLayout(false);
            this.panel2.PerformLayout();
            this.groupBox1.ResumeLayout(false);
            this.groupBox1.PerformLayout();
        private System.Windows.Forms.DataGridView dataGridView1;
        private System.Windows.Forms.Panel panel2;
        private System.Windows.Forms.Label lbl_birthday;
        private System.Windows.Forms.DateTimePicker dtp_birthday;
        private System.Windows.Forms.Panel panel3;
        private System.Windows.Forms.Panel panel1;
        private System.Windows.Forms.Panel panel4;
        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.Button btn_search;
            //cn.ConnectionString =
            //    @"Provider=Microsoft.ACE.OLEDB.12.0;Data Source=|DataDirectory|\SysDev.accdb;";
            //dataload();
        //private void dataload()   //カスタム関数
        //{
        //    // ID、Pass（パスワード）、Name(名前）
        //    // PostNumber（郵便番号）、Address（住所）  Memberテーブルから
        //    OleDbDataAdapter da =
        //        new OleDbDataAdapter("SELECT 顧客ID,氏名,性別,生年月日,郵便番号,住所,電話番号", cn);
        //    DataTable dt = new DataTable();
        //    da.Fill(dt);
        //    dataGridView1.DataSource = dt;
        //    dataGridView1.AllowUserToAddRows = false;    //最下行を非表示
        //    dataGridView1.AutoResizeColumns();           //列の幅の自動調整
        //    cmb_sex.DataSource = dt;
        //    cmb_sex.DisplayMember = "性別";
        //    cmb_sex.ValueMember = "性別";
        //    // コンボボックスにデータテーブルをセット
        //    /*this.comboB_CarName.DataSource = dt;
        //    this.comboB_CarName.DisplayMember = "商品名";
        //    this.comboB_CarName.ValueMember = "商品名";
        //    this.comboB_Maker.DataSource = dt;
        //    this.comboB_Maker.DisplayMember = "メーカー";
        //    this.comboB_Maker.ValueMember = "メーカー";*/
        //}
            //OleDbCommand cmd =
            //     new OleDbCommand("SELECT 顧客ID,氏名,性別,生年月日,郵便番号,住所,電話番号 " +
            //     "FROM 顧客マスタ WHERE 顧客ID LIKE '%'+ @氏名 + '%' ORDER BY 顧客ID");
            //cmd.Connection = cn;
            //OleDbDataAdapter da = new OleDbDataAdapter();
            //da.SelectCommand = cmd;
            //cmd.Parameters.AddWithValue("@氏名", txt_id.Text);
            //DataTable dt = new DataTable();
            //da.Fill(dt);
            //dataGridView1.DataSource = dt;
            //dataGridView1.AllowUserToAddRows = false;   //最下行を非表示
            //dataGridView1.AutoResizeColumns();          //列の幅の自動調整
﻿namespace SystemDev_KY_22.ユーザーコントロール
{
    partial class ClientList2
    {
        /// <summary> 
        /// 必要なデザイナー変数です。
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary> 
        /// 使用中のリソースをすべてクリーンアップします。
        /// </summary>
        /// <param name="disposing">マネージド リソースを破棄する場合は true を指定し、その他の場合は false を指定します。</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region コンポーネント デザイナーで生成されたコード

        /// <summary> 
        /// デザイナー サポートに必要なメソッドです。このメソッドの内容を 
        /// コード エディターで変更しないでください。
        /// </summary>
        private void InitializeComponent()
        {
            this.panel1 = new System.Windows.Forms.Panel();
            this.panel2 = new System.Windows.Forms.Panel();
            this.panel3 = new System.Windows.Forms.Panel();
            this.panel4 = new System.Windows.Forms.Panel();
            this.dataGridView1 = new System.Windows.Forms.DataGridView();
            this.lbl_id = new System.Windows.Forms.Label();
            this.lbl_name = new System.Windows.Forms.Label();
            this.lbl_sex = new System.Windows.Forms.Label();
            this.lbl_birthday = new System.Windows.Forms.Label();
            this.lbl_pos = new System.Windows.Forms.Label();
            this.lbl_address = new System.Windows.Forms.Label();
            this.lbl_tel = new System.Windows.Forms.Label();
            this.txt_id = new System.Windows.Forms.TextBox();
            this.txt_name = new System.Windows.Forms.TextBox();
            this.txt_pos = new System.Windows.Forms.TextBox();
            this.txt_address = new System.Windows.Forms.TextBox();
            this.txt_tel = new System.Windows.Forms.TextBox();
            this.dtp_birthday = new System.Windows.Forms.DateTimePicker();
            this.cmb_sex = new System.Windows.Forms.ComboBox();
            this.btn_update = new System.Windows.Forms.Button();
            this.panel1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.dataGridView1)).BeginInit();
            this.SuspendLayout();
            // 
            // panel1
            // 
            this.panel1.Controls.Add(this.btn_update);
            this.panel1.Controls.Add(this.cmb_sex);
            this.panel1.Controls.Add(this.dtp_birthday);
            this.panel1.Controls.Add(this.txt_tel);
            this.panel1.Controls.Add(this.txt_address);
            this.panel1.Controls.Add(this.txt_pos);
            this.panel1.Controls.Add(this.txt_name);
            this.panel1.Controls.Add(this.txt_id);
            this.panel1.Controls.Add(this.lbl_tel);
            this.panel1.Controls.Add(this.lbl_address);
            this.panel1.Controls.Add(this.lbl_pos);
            this.panel1.Controls.Add(this.lbl_birthday);
            this.panel1.Controls.Add(this.lbl_sex);
            this.panel1.Controls.Add(this.lbl_name);
            this.panel1.Controls.Add(this.lbl_id);
            this.panel1.Dock = System.Windows.Forms.DockStyle.Top;
            this.panel1.Font = new System.Drawing.Font("MS UI Gothic", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(128)));
            this.panel1.Location = new System.Drawing.Point(0, 0);
            this.panel1.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.panel1.Name = "panel1";
            this.panel1.Size = new System.Drawing.Size(1312, 128);
            this.panel1.TabIndex = 0;
            // 
            // panel2
            // 
            this.panel2.Dock = System.Windows.Forms.DockStyle.Left;
            this.panel2.Location = new System.Drawing.Point(0, 128);
            this.panel2.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.panel2.Name = "panel2";
            this.panel2.Size = new System.Drawing.Size(17, 555);
            this.panel2.TabIndex = 1;
            // 
            // panel3
            // 
            this.panel3.Dock = System.Windows.Forms.DockStyle.Right;
            this.panel3.Location = new System.Drawing.Point(1295, 128);
            this.panel3.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.panel3.Name = "panel3";
            this.panel3.Size = new System.Drawing.Size(17, 555);
            this.panel3.TabIndex = 2;
            // 
            // panel4
            // 
            this.panel4.Dock = System.Windows.Forms.DockStyle.Bottom;
            this.panel4.Location = new System.Drawing.Point(17, 672);
            this.panel4.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.panel4.Name = "panel4";
            this.panel4.Size = new System.Drawing.Size(1278, 11);
            this.panel4.TabIndex = 3;
            // 
            // dataGridView1
            // 
            this.dataGridView1.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dataGridView1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.dataGridView1.Location = new System.Drawing.Point(17, 128);
            this.dataGridView1.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.dataGridView1.Name = "dataGridView1";
            this.dataGridView1.RowTemplate.Height = 21;
            this.dataGridView1.Size = new System.Drawing.Size(1278, 544);
            this.dataGridView1.TabIndex = 4;
            // 
            // lbl_id
            // 
            this.lbl_id.AutoSize = true;
            this.lbl_id.Location = new System.Drawing.Point(13, 14);
            this.lbl_id.Name = "lbl_id";
            this.lbl_id.Size = new System.Drawing.Size(57, 16);
            this.lbl_id.TabIndex = 0;
            this.lbl_id.Text = "顧客ID:";
            // 
            // lbl_name
            // 
            this.lbl_name.AutoSize = true;
            this.lbl_name.Location = new System.Drawing.Point(27, 54);
            this.lbl_name.Name = "lbl_name";
            this.lbl_name.Size = new System.Drawing.Size(43, 16);
            this.lbl_name.TabIndex = 1;
            this.lbl_name.Text = "氏名:";
            // 
            // lbl_sex
            // 
            this.lbl_sex.AutoSize = true;
            this.lbl_sex.Location = new System.Drawing.Point(26, 97);
            this.lbl_sex.Name = "lbl_sex";
            this.lbl_sex.Size = new System.Drawing.Size(43, 16);
            this.lbl_sex.TabIndex = 2;
            this.lbl_sex.Text = "性別:";
            // 
            // lbl_birthday
            // 
            this.lbl_birthday.AutoSize = true;
            this.lbl_birthday.Location = new System.Drawing.Point(223, 11);
            this.lbl_birthday.Name = "lbl_birthday";
            this.lbl_birthday.Size = new System.Drawing.Size(75, 16);
            this.lbl_birthday.TabIndex = 3;
            this.lbl_birthday.Text = "生年月日:";
            // 
            // lbl_pos
            // 
            this.lbl_pos.AutoSize = true;
            this.lbl_pos.Location = new System.Drawing.Point(223, 52);
            this.lbl_pos.Name = "lbl_pos";
            this.lbl_pos.Size = new System.Drawing.Size(75, 16);
            this.lbl_pos.TabIndex = 4;
            this.lbl_pos.Text = "郵便番号:";
            // 
            // lbl_address
            // 
            this.lbl_address.AutoSize = true;
            this.lbl_address.Location = new System.Drawing.Point(251, 97);
            this.lbl_address.Name = "lbl_address";
            this.lbl_address.Size = new System.Drawing.Size(43, 16);
            this.lbl_address.TabIndex = 5;
            this.lbl_address.Text = "住所:";
            // 
            // lbl_tel
            // 
            this.lbl_tel.AutoSize = true;
            this.lbl_tel.Location = new System.Drawing.Point(469, 11);
            this.lbl_tel.Name = "lbl_tel";
            this.lbl_tel.Size = new System.Drawing.Size(75, 16);
            this.lbl_tel.TabIndex = 6;
            this.lbl_tel.Text = "電話番号:";
            // 
            // txt_id
            // 
            this.txt_id.Location = new System.Drawing.Point(69, 11);
            this.txt_id.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.txt_id.Name = "txt_id";
            this.txt_id.Size = new System.Drawing.Size(116, 23);
            this.txt_id.TabIndex = 7;
            // 
            // txt_name
            // 
            this.txt_name.Location = new System.Drawing.Point(69, 51);
            this.txt_name.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.txt_name.Name = "txt_name";
            this.txt_name.Size = new System.Drawing.Size(116, 23);
            this.txt_name.TabIndex = 8;
            // 
            // txt_pos
            // 
            this.txt_pos.Location = new System.Drawing.Point(294, 49);
            this.txt_pos.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.txt_pos.Name = "txt_pos";
            this.txt_pos.Size = new System.Drawing.Size(116, 23);
            this.txt_pos.TabIndex = 11;
            // 
            // txt_address
            // 
            this.txt_address.Location = new System.Drawing.Point(294, 94);
            this.txt_address.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.txt_address.Name = "txt_address";
            this.txt_address.Size = new System.Drawing.Size(216, 23);
            this.txt_address.TabIndex = 12;
            // 
            // txt_tel
            // 
            this.txt_tel.Location = new System.Drawing.Point(540, 8);
            this.txt_tel.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.txt_tel.Name = "txt_tel";
            this.txt_tel.Size = new System.Drawing.Size(135, 23);
            this.txt_tel.TabIndex = 13;
            // 
            // dtp_birthday
            // 
            this.dtp_birthday.Location = new System.Drawing.Point(294, 8);
            this.dtp_birthday.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.dtp_birthday.Name = "dtp_birthday";
            this.dtp_birthday.Size = new System.Drawing.Size(143, 23);
            this.dtp_birthday.TabIndex = 14;
            // 
            // cmb_sex
            // 
            this.cmb_sex.FormattingEnabled = true;
            this.cmb_sex.Items.AddRange(new object[] {
            "男性",
            "女性"});
            this.cmb_sex.Location = new System.Drawing.Point(69, 93);
            this.cmb_sex.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.cmb_sex.Name = "cmb_sex";
            this.cmb_sex.Size = new System.Drawing.Size(116, 24);
            this.cmb_sex.TabIndex = 15;
            // 
            // btn_update
            // 
            this.btn_update.Location = new System.Drawing.Point(565, 98);
            this.btn_update.Name = "btn_update";
            this.btn_update.Size = new System.Drawing.Size(75, 23);
            this.btn_update.TabIndex = 16;
            this.btn_update.Text = "更新";
            this.btn_update.UseVisualStyleBackColor = true;
            // 
            // ClientList2
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(7F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.dataGridView1);
            this.Controls.Add(this.panel4);
            this.Controls.Add(this.panel3);
            this.Controls.Add(this.panel2);
            this.Controls.Add(this.panel1);
            this.Font = new System.Drawing.Font("MS UI Gothic", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(128)));
            this.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.Name = "ClientList2";
            this.Size = new System.Drawing.Size(1312, 683);
            this.Load += new System.EventHandler(this.ClientList2_Load);
            this.panel1.ResumeLayout(false);
            this.panel1.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)(this.dataGridView1)).EndInit();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Panel panel1;
        private System.Windows.Forms.Panel panel2;
        private System.Windows.Forms.Panel panel3;
        private System.Windows.Forms.Panel panel4;
        private System.Windows.Forms.DataGridView dataGridView1;
        private System.Windows.Forms.Label lbl_birthday;
        private System.Windows.Forms.Label lbl_sex;
        private System.Windows.Forms.Label lbl_name;
        private System.Windows.Forms.Label lbl_id;
        private System.Windows.Forms.Label lbl_tel;
        private System.Windows.Forms.Label lbl_address;
        private System.Windows.Forms.Label lbl_pos;
        private System.Windows.Forms.TextBox txt_tel;
        private System.Windows.Forms.TextBox txt_address;
        private System.Windows.Forms.TextBox txt_pos;
        private System.Windows.Forms.TextBox txt_name;
        private System.Windows.Forms.TextBox txt_id;
        private System.Windows.Forms.DateTimePicker dtp_birthday;
        private System.Windows.Forms.ComboBox cmb_sex;
        private System.Windows.Forms.Button btn_update;
    }
}
﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Data.OleDb;

namespace SystemDev_KY_22.ユーザーコントロール
{
    public partial class ClientList2 : UserControl
    {
        OleDbConnection cn = new OleDbConnection();  //コネクションオブジェクト

        public ClientList2()
        {
            InitializeComponent();
        }

        private void ClientList2_Load(object sender, EventArgs e)
        {
            cn.ConnectionString =
                @"Provider=Microsoft.ACE.OLEDB.12.0;Data Source=|DataDirectory|\SysDev.accdb;";
            dataload();
        }

        private void dataload()   //カスタム関数
        {
            OleDbDataAdapter da =
                new OleDbDataAdapter("SELECT 顧客ID, 氏名, 性別, 生年月日, 郵便番号, 住所, 電話番号 FROM 顧客マスタ ORDER BY 顧客ID", cn);
            DataTable dt = new DataTable();
            da.Fill(dt);
            dataGridView1.DataSource = dt;
            dataGridView1.AllowUserToAddRows = false;    //最下行を非表示
            dataGridView1.AutoResizeColumns();           //列の幅の自動調整
        }
    }
}
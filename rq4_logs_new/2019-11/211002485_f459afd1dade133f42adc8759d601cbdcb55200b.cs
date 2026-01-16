namespace SystemDev_KY_22
{
    partial class ClientList
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
            this.cmb_sex = new System.Windows.Forms.ComboBox();
            this.txt_pos = new System.Windows.Forms.TextBox();
            this.lbl_pos = new System.Windows.Forms.Label();
            this.txt_id = new System.Windows.Forms.TextBox();
            this.lbl_id = new System.Windows.Forms.Label();
            this.txt_tel = new System.Windows.Forms.TextBox();
            this.lbl_tel = new System.Windows.Forms.Label();
            this.lbl_address = new System.Windows.Forms.Label();
            this.txt_address = new System.Windows.Forms.TextBox();
            this.lbl_birthday = new System.Windows.Forms.Label();
            this.dtp_birthday = new System.Windows.Forms.DateTimePicker();
            this.lbl_name = new System.Windows.Forms.Label();
            this.lbl_sex = new System.Windows.Forms.Label();
            this.txt_name = new System.Windows.Forms.TextBox();
            this.dataGridView1 = new System.Windows.Forms.DataGridView();
            ((System.ComponentModel.ISupportInitialize)(this.dataGridView1)).BeginInit();
            this.SuspendLayout();
            // 
            // cmb_sex
            // 
            this.cmb_sex.Font = new System.Drawing.Font("MS UI Gothic", 18F);
            this.cmb_sex.FormattingEnabled = true;
            this.cmb_sex.Items.AddRange(new object[] {
            "男性",
            "女性"});
            this.cmb_sex.Location = new System.Drawing.Point(144, 77);
            this.cmb_sex.Name = "cmb_sex";
            this.cmb_sex.Size = new System.Drawing.Size(213, 38);
            this.cmb_sex.TabIndex = 39;
            // 
            // txt_pos
            // 
            this.txt_pos.Font = new System.Drawing.Font("MS UI Gothic", 18F);
            this.txt_pos.Location = new System.Drawing.Point(144, 140);
            this.txt_pos.Margin = new System.Windows.Forms.Padding(4);
            this.txt_pos.Name = "txt_pos";
            this.txt_pos.Size = new System.Drawing.Size(290, 37);
            this.txt_pos.TabIndex = 38;
            // 
            // lbl_pos
            // 
            this.lbl_pos.AutoSize = true;
            this.lbl_pos.Font = new System.Drawing.Font("MS UI Gothic", 18F);
            this.lbl_pos.Location = new System.Drawing.Point(4, 143);
            this.lbl_pos.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lbl_pos.Name = "lbl_pos";
            this.lbl_pos.Size = new System.Drawing.Size(148, 30);
            this.lbl_pos.TabIndex = 37;
            this.lbl_pos.Text = "郵便番号：";
            // 
            // txt_id
            // 
            this.txt_id.Font = new System.Drawing.Font("MS UI Gothic", 18F);
            this.txt_id.Location = new System.Drawing.Point(617, 11);
            this.txt_id.Margin = new System.Windows.Forms.Padding(4);
            this.txt_id.Name = "txt_id";
            this.txt_id.Size = new System.Drawing.Size(290, 37);
            this.txt_id.TabIndex = 36;
            // 
            // lbl_id
            // 
            this.lbl_id.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom)));
            this.lbl_id.AutoSize = true;
            this.lbl_id.Font = new System.Drawing.Font("MS UI Gothic", 18F);
            this.lbl_id.Location = new System.Drawing.Point(23, 14);
            this.lbl_id.Name = "lbl_id";
            this.lbl_id.Size = new System.Drawing.Size(114, 30);
            this.lbl_id.TabIndex = 35;
            this.lbl_id.Text = "顧客ID：";
            // 
            // txt_tel
            // 
            this.txt_tel.Font = new System.Drawing.Font("MS UI Gothic", 18F);
            this.txt_tel.Location = new System.Drawing.Point(145, 191);
            this.txt_tel.Margin = new System.Windows.Forms.Padding(4);
            this.txt_tel.Name = "txt_tel";
            this.txt_tel.Size = new System.Drawing.Size(290, 37);
            this.txt_tel.TabIndex = 34;
            // 
            // lbl_tel
            // 
            this.lbl_tel.AutoSize = true;
            this.lbl_tel.Font = new System.Drawing.Font("MS UI Gothic", 18F);
            this.lbl_tel.Location = new System.Drawing.Point(67, 194);
            this.lbl_tel.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lbl_tel.Name = "lbl_tel";
            this.lbl_tel.Size = new System.Drawing.Size(70, 30);
            this.lbl_tel.TabIndex = 33;
            this.lbl_tel.Text = "TEL:";
            // 
            // lbl_address
            // 
            this.lbl_address.AutoSize = true;
            this.lbl_address.Font = new System.Drawing.Font("MS UI Gothic", 18F);
            this.lbl_address.Location = new System.Drawing.Point(521, 143);
            this.lbl_address.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lbl_address.Name = "lbl_address";
            this.lbl_address.Size = new System.Drawing.Size(79, 30);
            this.lbl_address.TabIndex = 32;
            this.lbl_address.Text = "住所:";
            // 
            // txt_address
            // 
            this.txt_address.Font = new System.Drawing.Font("MS UI Gothic", 18F);
            this.txt_address.Location = new System.Drawing.Point(617, 140);
            this.txt_address.Margin = new System.Windows.Forms.Padding(4);
            this.txt_address.Name = "txt_address";
            this.txt_address.Size = new System.Drawing.Size(290, 37);
            this.txt_address.TabIndex = 31;
            // 
            // lbl_birthday
            // 
            this.lbl_birthday.AutoSize = true;
            this.lbl_birthday.Font = new System.Drawing.Font("MS UI Gothic", 18F);
            this.lbl_birthday.Location = new System.Drawing.Point(470, 80);
            this.lbl_birthday.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lbl_birthday.Name = "lbl_birthday";
            this.lbl_birthday.Size = new System.Drawing.Size(139, 30);
            this.lbl_birthday.TabIndex = 30;
            this.lbl_birthday.Text = "生年月日:";
            // 
            // dtp_birthday
            // 
            this.dtp_birthday.CalendarFont = new System.Drawing.Font("MS UI Gothic", 24F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(128)));
            this.dtp_birthday.Font = new System.Drawing.Font("MS UI Gothic", 18F);
            this.dtp_birthday.Location = new System.Drawing.Point(617, 75);
            this.dtp_birthday.Margin = new System.Windows.Forms.Padding(4);
            this.dtp_birthday.Name = "dtp_birthday";
            this.dtp_birthday.Size = new System.Drawing.Size(290, 37);
            this.dtp_birthday.TabIndex = 29;
            // 
            // lbl_name
            // 
            this.lbl_name.AutoSize = true;
            this.lbl_name.Font = new System.Drawing.Font("MS UI Gothic", 18F);
            this.lbl_name.Location = new System.Drawing.Point(530, 14);
            this.lbl_name.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lbl_name.Name = "lbl_name";
            this.lbl_name.Size = new System.Drawing.Size(79, 30);
            this.lbl_name.TabIndex = 27;
            this.lbl_name.Text = "氏名:";
            // 
            // lbl_sex
            // 
            this.lbl_sex.AutoSize = true;
            this.lbl_sex.Font = new System.Drawing.Font("MS UI Gothic", 18F);
            this.lbl_sex.Location = new System.Drawing.Point(49, 80);
            this.lbl_sex.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lbl_sex.Name = "lbl_sex";
            this.lbl_sex.Size = new System.Drawing.Size(79, 30);
            this.lbl_sex.TabIndex = 28;
            this.lbl_sex.Text = "性別:";
            // 
            // txt_name
            // 
            this.txt_name.Font = new System.Drawing.Font("MS UI Gothic", 18F);
            this.txt_name.Location = new System.Drawing.Point(144, 11);
            this.txt_name.Margin = new System.Windows.Forms.Padding(4);
            this.txt_name.Name = "txt_name";
            this.txt_name.Size = new System.Drawing.Size(290, 37);
            this.txt_name.TabIndex = 26;
            // 
            // dataGridView1
            // 
            this.dataGridView1.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dataGridView1.Location = new System.Drawing.Point(3, 251);
            this.dataGridView1.Name = "dataGridView1";
            this.dataGridView1.RowTemplate.Height = 24;
            this.dataGridView1.Size = new System.Drawing.Size(1308, 514);
            this.dataGridView1.TabIndex = 40;
            // 
            // ClientList
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 15F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.dataGridView1);
            this.Controls.Add(this.cmb_sex);
            this.Controls.Add(this.txt_pos);
            this.Controls.Add(this.lbl_pos);
            this.Controls.Add(this.txt_id);
            this.Controls.Add(this.lbl_id);
            this.Controls.Add(this.txt_tel);
            this.Controls.Add(this.lbl_tel);
            this.Controls.Add(this.lbl_address);
            this.Controls.Add(this.txt_address);
            this.Controls.Add(this.lbl_birthday);
            this.Controls.Add(this.dtp_birthday);
            this.Controls.Add(this.lbl_name);
            this.Controls.Add(this.lbl_sex);
            this.Controls.Add(this.txt_name);
            this.Name = "ClientList";
            this.Size = new System.Drawing.Size(1314, 768);
            this.Load += new System.EventHandler(this.ClientList_Load);
            ((System.ComponentModel.ISupportInitialize)(this.dataGridView1)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.ComboBox cmb_sex;
        private System.Windows.Forms.TextBox txt_pos;
        private System.Windows.Forms.Label lbl_pos;
        private System.Windows.Forms.TextBox txt_id;
        private System.Windows.Forms.Label lbl_id;
        private System.Windows.Forms.TextBox txt_tel;
        private System.Windows.Forms.Label lbl_tel;
        private System.Windows.Forms.Label lbl_address;
        private System.Windows.Forms.TextBox txt_address;
        private System.Windows.Forms.Label lbl_birthday;
        private System.Windows.Forms.DateTimePicker dtp_birthday;
        private System.Windows.Forms.Label lbl_name;
        private System.Windows.Forms.Label lbl_sex;
        private System.Windows.Forms.TextBox txt_name;
        private System.Windows.Forms.DataGridView dataGridView1;
    }
}
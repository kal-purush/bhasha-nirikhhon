namespace winform_chat.DashboardForm
{
    partial class PvpModeForm
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
            btn_createRoom = new Button();
            label1 = new Label();
            btn_autoJoin = new Button();
            label2 = new Label();
            comboBox1 = new ComboBox();
            listBoxPlayerRooms = new ListBox();
            listview_userQueue = new ListView();
            SuspendLayout();
            // 
            // btn_createRoom
            // 
            btn_createRoom.Font = new Font("Segoe UI", 13F);
            btn_createRoom.Location = new Point(533, 291);
            btn_createRoom.Margin = new Padding(3, 2, 3, 2);
            btn_createRoom.Name = "btn_createRoom";
            btn_createRoom.Size = new Size(186, 45);
            btn_createRoom.TabIndex = 2;
            btn_createRoom.Text = "Create Room";
            btn_createRoom.UseVisualStyleBackColor = true;
            btn_createRoom.Click += btn_createRoom_Click;
            // 
            // label1
            // 
            label1.AutoSize = true;
            label1.Font = new Font("Segoe UI", 13F);
            label1.Location = new Point(22, 27);
            label1.Name = "label1";
            label1.Size = new Size(107, 25);
            label1.TabIndex = 3;
            label1.Text = "Select room";
            // 
            // btn_autoJoin
            // 
            btn_autoJoin.Font = new Font("Segoe UI", 13F);
            btn_autoJoin.Location = new Point(533, 349);
            btn_autoJoin.Margin = new Padding(3, 2, 3, 2);
            btn_autoJoin.Name = "btn_autoJoin";
            btn_autoJoin.Size = new Size(186, 45);
            btn_autoJoin.TabIndex = 4;
            btn_autoJoin.Text = "🔄 Auto matching";
            btn_autoJoin.UseVisualStyleBackColor = true;
            btn_autoJoin.Click += btn_autoJoin_Click;
            // 
            // label2
            // 
            label2.AutoSize = true;
            label2.Font = new Font("Segoe UI", 13F);
            label2.Location = new Point(533, 27);
            label2.Name = "label2";
            label2.Size = new Size(110, 25);
            label2.TabIndex = 5;
            label2.Text = "Game mode";
            // 
            // comboBox1
            // 
            comboBox1.DropDownStyle = ComboBoxStyle.DropDownList;
            comboBox1.Font = new Font("Segoe UI", 15.75F, FontStyle.Regular, GraphicsUnit.Point, 0);
            comboBox1.Location = new Point(533, 65);
            comboBox1.Name = "comboBox1";
            comboBox1.Size = new Size(186, 38);
            comboBox1.TabIndex = 7;
            // 
            // listBoxPlayerRooms
            // 
            listBoxPlayerRooms.Font = new Font("Segoe UI Emoji", 18F, FontStyle.Regular, GraphicsUnit.Point, 0);
            listBoxPlayerRooms.FormattingEnabled = true;
            listBoxPlayerRooms.ItemHeight = 32;
            listBoxPlayerRooms.Location = new Point(22, 55);
            listBoxPlayerRooms.Name = "listBoxPlayerRooms";
            listBoxPlayerRooms.Size = new Size(464, 324);
            listBoxPlayerRooms.TabIndex = 8;
            // 
            // listview_userQueue
            // 
            listview_userQueue.Location = new Point(543, 123);
            listview_userQueue.Margin = new Padding(3, 2, 3, 2);
            listview_userQueue.Name = "listview_userQueue";
            listview_userQueue.Size = new Size(138, 129);
            listview_userQueue.TabIndex = 1;
            listview_userQueue.UseCompatibleStateImageBehavior = false;
            // 
            // PvpModeForm
            // 
            AutoScaleDimensions = new SizeF(7F, 15F);
            AutoScaleMode = AutoScaleMode.Font;
            ClientSize = new Size(767, 440);
            Controls.Add(listBoxPlayerRooms);
            Controls.Add(comboBox1);
            Controls.Add(label2);
            Controls.Add(btn_autoJoin);
            Controls.Add(label1);
            Controls.Add(btn_createRoom);
            Controls.Add(listview_userQueue);
            Margin = new Padding(3, 2, 3, 2);
            Name = "PvpModeForm";
            Text = "ChatServer";
            FormClosed += ClientForm_FormClosed;
            Load += ClientForm_Load;
            ResumeLayout(false);
            PerformLayout();
        }

        #endregion
        private Button btn_createRoom;
        private Label label1;
        private Button btn_autoJoin;
        private Label label2;
        private ComboBox comboBox1;
        private ListBox listBoxPlayerRooms;
        private ListView listview_userQueue;
    }
}
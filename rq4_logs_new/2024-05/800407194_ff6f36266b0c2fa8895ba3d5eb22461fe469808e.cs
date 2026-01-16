namespace ChessAI_Bck
{
    partial class PromotePawnForm
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
            components = new System.ComponentModel.Container();
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(PromotePawnForm));
            QueenButton = new Button();
            RookButton = new Button();
            KnightButton = new Button();
            BishopButton = new Button();
            imageList2 = new ImageList(components);
            imageList1 = new ImageList(components);
            promoteText = new Label();
            SuspendLayout();
            // 
            // QueenButton
            // 
            QueenButton.ImageKey = "WQueen";
            QueenButton.ImageList = imageList1;
            QueenButton.Location = new Point(17, 11);
            QueenButton.Margin = new Padding(3, 2, 3, 2);
            QueenButton.Name = "QueenButton";
            QueenButton.Size = new Size(113, 98);
            QueenButton.TabIndex = 5;
            QueenButton.Text = " ";
            QueenButton.UseVisualStyleBackColor = true;
            // 
            // RookButton
            // 
            RookButton.ImageKey = "WRook";
            RookButton.ImageList = imageList1;
            RookButton.Location = new Point(136, 11);
            RookButton.Margin = new Padding(3, 2, 3, 2);
            RookButton.Name = "RookButton";
            RookButton.Size = new Size(113, 98);
            RookButton.TabIndex = 6;
            RookButton.Text = " ";
            RookButton.UseVisualStyleBackColor = true;
            // 
            // KnightButton
            // 
            KnightButton.ImageKey = "WKnight";
            KnightButton.ImageList = imageList1;
            KnightButton.Location = new Point(255, 11);
            KnightButton.Margin = new Padding(3, 2, 3, 2);
            KnightButton.Name = "KnightButton";
            KnightButton.Size = new Size(113, 98);
            KnightButton.TabIndex = 7;
            KnightButton.Text = " ";
            KnightButton.UseVisualStyleBackColor = true;
            // 
            // BishopButton
            // 
            BishopButton.ImageKey = "WBishop";
            BishopButton.ImageList = imageList1;
            BishopButton.Location = new Point(374, 11);
            BishopButton.Margin = new Padding(3, 2, 3, 2);
            BishopButton.Name = "BishopButton";
            BishopButton.Size = new Size(106, 98);
            BishopButton.TabIndex = 8;
            BishopButton.Text = " ";
            BishopButton.UseVisualStyleBackColor = true;
            // 
            // imageList2
            // 
            imageList2.ColorDepth = ColorDepth.Depth8Bit;
            imageList2.ImageStream = (ImageListStreamer)resources.GetObject("imageList2.ImageStream");
            imageList2.TransparentColor = Color.Transparent;
            imageList2.Images.SetKeyName(0, "BBishop");
            imageList2.Images.SetKeyName(1, "BKnight");
            imageList2.Images.SetKeyName(2, "BQueen");
            imageList2.Images.SetKeyName(3, "BRook");
            imageList2.Images.SetKeyName(4, "WBishop");
            imageList2.Images.SetKeyName(5, "WKnight");
            imageList2.Images.SetKeyName(6, "WQueen");
            imageList2.Images.SetKeyName(7, "WRook");
            // 
            // imageList1
            // 
            imageList1.ColorDepth = ColorDepth.Depth8Bit;
            imageList1.ImageStream = (ImageListStreamer)resources.GetObject("imageList1.ImageStream");
            imageList1.TransparentColor = Color.Transparent;
            imageList1.Images.SetKeyName(0, "BBishop");
            imageList1.Images.SetKeyName(1, "BKnight");
            imageList1.Images.SetKeyName(2, "BQueen");
            imageList1.Images.SetKeyName(3, "BRook");
            imageList1.Images.SetKeyName(4, "WBishop");
            imageList1.Images.SetKeyName(5, "WKnight");
            imageList1.Images.SetKeyName(6, "WQueen");
            imageList1.Images.SetKeyName(7, "WRook");
            // 
            // promoteText
            // 
            promoteText.AutoSize = true;
            promoteText.Font = new Font("Segoe UI", 14.25F, FontStyle.Regular, GraphicsUnit.Point, 163);
            promoteText.Location = new Point(8, 114);
            promoteText.Name = "promoteText";
            promoteText.Size = new Size(482, 50);
            promoteText.TabIndex = 9;
            promoteText.Text = "Congratulations!🎉 Your pawn is ready to be promoted. \r\nChoose one of the following pieces:";
            // 
            // PromotePawnForm
            // 
            AutoScaleDimensions = new SizeF(7F, 15F);
            AutoScaleMode = AutoScaleMode.Font;
            ClientSize = new Size(494, 173);
            Controls.Add(promoteText);
            Controls.Add(QueenButton);
            Controls.Add(RookButton);
            Controls.Add(BishopButton);
            Controls.Add(KnightButton);
            Name = "PromotePawnForm";
            Text = "Promote Your Pawn";
            ResumeLayout(false);
            PerformLayout();
        }

        #endregion
        private Button QueenButton;
        private Button RookButton;
        private Button KnightButton;
        private Button BishopButton;
        private ImageList imageList2;
        private ImageList imageList1;
        private Label promoteText;
    }
}
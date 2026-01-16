namespace GraphicsEditorNamespace
{
    partial class GraphicsEditor
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
            this.pictureBox = new System.Windows.Forms.PictureBox();
            this.buttonsTable = new System.Windows.Forms.TableLayoutPanel();
            this.addButton = new System.Windows.Forms.Button();
            this.removeButton = new System.Windows.Forms.Button();
            this.undoButton = new System.Windows.Forms.Button();
            this.redoButton = new System.Windows.Forms.Button();
            this.manipulateButton = new System.Windows.Forms.Button();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox)).BeginInit();
            this.buttonsTable.SuspendLayout();
            this.SuspendLayout();
            // 
            // pictureBox
            // 
            this.pictureBox.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.pictureBox.BackColor = System.Drawing.SystemColors.ButtonFace;
            this.pictureBox.Location = new System.Drawing.Point(61, 0);
            this.pictureBox.Name = "pictureBox";
            this.pictureBox.Size = new System.Drawing.Size(221, 260);
            this.pictureBox.TabIndex = 0;
            this.pictureBox.TabStop = false;
            this.pictureBox.Paint += new System.Windows.Forms.PaintEventHandler(this.pictureBoxPaintHandler);
            this.pictureBox.MouseClick += new System.Windows.Forms.MouseEventHandler(this.pictureBoxMouseClickHandler);
            this.pictureBox.MouseMove += new System.Windows.Forms.MouseEventHandler(this.pictureBoxMouseMoveHandler);
            // 
            // buttonsTable
            // 
            this.buttonsTable.BackColor = System.Drawing.SystemColors.HotTrack;
            this.buttonsTable.ColumnCount = 1;
            this.buttonsTable.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.buttonsTable.Controls.Add(this.addButton, 0, 0);
            this.buttonsTable.Controls.Add(this.removeButton, 0, 1);
            this.buttonsTable.Controls.Add(this.undoButton, 0, 2);
            this.buttonsTable.Controls.Add(this.redoButton, 0, 3);
            this.buttonsTable.Controls.Add(this.manipulateButton, 0, 4);
            this.buttonsTable.Dock = System.Windows.Forms.DockStyle.Left;
            this.buttonsTable.Location = new System.Drawing.Point(0, 0);
            this.buttonsTable.MaximumSize = new System.Drawing.Size(60, 261);
            this.buttonsTable.MinimumSize = new System.Drawing.Size(60, 261);
            this.buttonsTable.Name = "buttonsTable";
            this.buttonsTable.RowCount = 5;
            this.buttonsTable.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 20F));
            this.buttonsTable.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 20F));
            this.buttonsTable.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 20F));
            this.buttonsTable.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 20F));
            this.buttonsTable.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 20F));
            this.buttonsTable.Size = new System.Drawing.Size(60, 261);
            this.buttonsTable.TabIndex = 1;
            // 
            // addButton
            // 
            this.addButton.Location = new System.Drawing.Point(3, 3);
            this.addButton.Name = "addButton";
            this.addButton.Size = new System.Drawing.Size(52, 46);
            this.addButton.TabIndex = 0;
            this.addButton.Text = "add";
            this.addButton.UseVisualStyleBackColor = true;
            this.addButton.Click += new System.EventHandler(this.buttonClickHandler);
            // 
            // removeButton
            // 
            this.removeButton.Location = new System.Drawing.Point(3, 55);
            this.removeButton.Name = "removeButton";
            this.removeButton.Size = new System.Drawing.Size(52, 46);
            this.removeButton.TabIndex = 1;
            this.removeButton.Text = "remove";
            this.removeButton.UseVisualStyleBackColor = true;
            this.removeButton.Click += new System.EventHandler(this.buttonClickHandler);
            // 
            // undoButton
            // 
            this.undoButton.Location = new System.Drawing.Point(3, 107);
            this.undoButton.Name = "undoButton";
            this.undoButton.Size = new System.Drawing.Size(52, 46);
            this.undoButton.TabIndex = 2;
            this.undoButton.Text = "undo";
            this.undoButton.UseVisualStyleBackColor = true;
            this.undoButton.Click += new System.EventHandler(this.buttonClickHandler);
            // 
            // redoButton
            // 
            this.redoButton.Location = new System.Drawing.Point(3, 159);
            this.redoButton.Name = "redoButton";
            this.redoButton.Size = new System.Drawing.Size(52, 46);
            this.redoButton.TabIndex = 3;
            this.redoButton.Text = "redo";
            this.redoButton.UseVisualStyleBackColor = true;
            this.redoButton.Click += new System.EventHandler(this.buttonClickHandler);
            // 
            // manipulateButton
            // 
            this.manipulateButton.Location = new System.Drawing.Point(3, 211);
            this.manipulateButton.Name = "manipulateButton";
            this.manipulateButton.Size = new System.Drawing.Size(52, 46);
            this.manipulateButton.TabIndex = 4;
            this.manipulateButton.Text = "manipulate";
            this.manipulateButton.UseVisualStyleBackColor = true;
            this.manipulateButton.Click += new System.EventHandler(this.buttonClickHandler);
            // 
            // GraphicsEditor
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(284, 261);
            this.Controls.Add(this.buttonsTable);
            this.Controls.Add(this.pictureBox);
            this.MinimumSize = new System.Drawing.Size(300, 300);
            this.Name = "GraphicsEditor";
            this.Text = "Graphics editor";
            ((System.ComponentModel.ISupportInitialize)(this.pictureBox)).EndInit();
            this.buttonsTable.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.PictureBox pictureBox;
        private System.Windows.Forms.TableLayoutPanel buttonsTable;
        private System.Windows.Forms.Button addButton;
        private System.Windows.Forms.Button removeButton;
        private System.Windows.Forms.Button undoButton;
        private System.Windows.Forms.Button redoButton;
        private System.Windows.Forms.Button manipulateButton;
    }
}

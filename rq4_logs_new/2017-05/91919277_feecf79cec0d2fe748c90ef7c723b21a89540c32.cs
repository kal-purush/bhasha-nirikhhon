namespace Warehouse
{
    partial class Form1
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
            this.label1 = new System.Windows.Forms.Label();
            this.panel1 = new System.Windows.Forms.Panel();
            this.label2 = new System.Windows.Forms.Label();
            this.button1 = new System.Windows.Forms.Button();
            this.button2 = new System.Windows.Forms.Button();
            this.panel2 = new System.Windows.Forms.Panel();
            this.panel1.SuspendLayout();
            this.panel2.SuspendLayout();
            this.flowDoc = new System.Windows.Documents.FlowDocument();
            this.table1 = new System.Windows.Documents.Table();
            this.SuspendLayout();
            //
            //table1
            //
            flowDoc.Blocks.Add(table1);
            table1.CellSpacing = 10;
            table1.Background = System.Windows.Media.Brushes.White;
            int numberOfColumns = 5;
            for (int x = 0; x < numberOfColumns; x++)
            {
                table1.Columns.Add(new System.Windows.Documents.TableColumn());
                if (x % 2 == 0)
                    table1.Columns[x].Background = System.Windows.Media.Brushes.Beige;
                else
                    table1.Columns[x].Background = System.Windows.Media.Brushes.LightSteelBlue;
            }
            table1.RowGroups.Add(new System.Windows.Documents.TableRowGroup());
            table1.RowGroups[0].Rows.Add(new System.Windows.Documents.TableRow());
            currentRow = table1.RowGroups[0].Rows[0];
            currentRow.Background = System.Windows.Media.Brushes.Silver;
            currentRow.FontSize = 40;
            currentRow.FontWeight = System.Windows.FontWeights.Bold;
            currentRow.Cells.Add(new System.Windows.Documents.TableCell(
                                    new System.Windows.Documents.Paragraph(
                                        new System.Windows.Documents.Run("List of Orders"))));
            currentRow.Cells[0].ColumnSpan = 5;
            table1.RowGroups[0].Rows.Add(new System.Windows.Documents.TableRow());
            currentRow = table1.RowGroups[0].Rows[1];
            currentRow.FontSize = 18;
            currentRow.FontWeight = System.Windows.FontWeights.Bold;
            currentRow.Cells.Add(new System.Windows.Documents.TableCell(
                                    new System.Windows.Documents.Paragraph(
                                        new System.Windows.Documents.Run("Order ID"))));
            currentRow.Cells.Add(new System.Windows.Documents.TableCell(
                                    new System.Windows.Documents.Paragraph(
                                        new System.Windows.Documents.Run("Book ID"))));
            currentRow.Cells.Add(new System.Windows.Documents.TableCell(
                                    new System.Windows.Documents.Paragraph(
                                        new System.Windows.Documents.Run("Book Title"))));
            currentRow.Cells.Add(new System.Windows.Documents.TableCell(
                                    new System.Windows.Documents.Paragraph(
                                        new System.Windows.Documents.Run("Quantity"))));
            currentRow.Cells.Add(new System.Windows.Documents.TableCell(
                                    new System.Windows.Documents.Paragraph(
                                        new System.Windows.Documents.Run("Status"))));



            // 
            // label1
            // 
            this.label1.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.label1.Font = new System.Drawing.Font("Times New Roman", 22.2F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.ForeColor = System.Drawing.SystemColors.ControlText;
            this.label1.Location = new System.Drawing.Point(12, 9);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(535, 83);
            this.label1.TabIndex = 0;
            this.label1.Text = "Warehouse";
            this.label1.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // panel1
            // 
            this.panel1.Controls.Add(this.label2);
            this.panel1.Location = new System.Drawing.Point(12, 106);
            this.panel1.Name = "panel1";
            this.panel1.Size = new System.Drawing.Size(535, 324);
            this.panel1.TabIndex = 1;
            // 
            // label2
            // 
            this.label2.Font = new System.Drawing.Font("Times New Roman", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label2.Location = new System.Drawing.Point(3, 0);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(81, 26);
            this.label2.TabIndex = 0;
            this.label2.Text = "Orders";
            // 
            // button1
            // 
            this.button1.Location = new System.Drawing.Point(101, 17);
            this.button1.Name = "button1";
            this.button1.Size = new System.Drawing.Size(153, 46);
            this.button1.TabIndex = 2;
            this.button1.Text = "Refresh";
            this.button1.UseVisualStyleBackColor = true;
            // 
            // button2
            // 
            this.button2.Location = new System.Drawing.Point(285, 17);
            this.button2.Name = "button2";
            this.button2.Size = new System.Drawing.Size(153, 46);
            this.button2.TabIndex = 3;
            this.button2.Text = "Complete Order";
            this.button2.UseVisualStyleBackColor = true;
            // 
            // panel2
            // 
            this.panel2.Controls.Add(this.button1);
            this.panel2.Controls.Add(this.button2);
            this.panel2.Location = new System.Drawing.Point(12, 436);
            this.panel2.Name = "panel2";
            this.panel2.Size = new System.Drawing.Size(535, 83);
            this.panel2.TabIndex = 4;
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.Color.AliceBlue;
            this.ClientSize = new System.Drawing.Size(559, 531);
            this.Controls.Add(this.panel2);
            this.Controls.Add(this.panel1);
            this.Controls.Add(this.label1);
            this.Name = "Form1";
            this.Text = "Form1";
            this.panel1.ResumeLayout(false);
            this.panel2.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Panel panel1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Button button1;
        private System.Windows.Forms.Button button2;
        private System.Windows.Forms.Panel panel2;
        private System.Windows.Documents.FlowDocument flowDoc;
        private System.Windows.Documents.Table table1;
        private System.Windows.Documents.TableRow currentRow;
    }
}

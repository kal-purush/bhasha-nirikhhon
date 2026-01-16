namespace Fall2020_CSC403_Project
{
  partial class FrmInventory
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
      this.listViewInventory = new System.Windows.Forms.ListView();
      this.colItemName = ((System.Windows.Forms.ColumnHeader)(new System.Windows.Forms.ColumnHeader()));
      this.colType = ((System.Windows.Forms.ColumnHeader)(new System.Windows.Forms.ColumnHeader()));
      this.colValue = ((System.Windows.Forms.ColumnHeader)(new System.Windows.Forms.ColumnHeader()));
      this.label1 = new System.Windows.Forms.Label();
      this.SuspendLayout();
      // 
      // listViewInventory
      // 
      this.listViewInventory.Columns.AddRange(new System.Windows.Forms.ColumnHeader[] {
            this.colItemName,
            this.colType,
            this.colValue});
      this.listViewInventory.Location = new System.Drawing.Point(332, 130);
      this.listViewInventory.Name = "listViewInventory";
      this.listViewInventory.Size = new System.Drawing.Size(345, 195);
      this.listViewInventory.TabIndex = 2;
      this.listViewInventory.UseCompatibleStateImageBehavior = false;
      // 
      // colItemName
      // 
      this.colItemName.Text = "Name";
      // 
      // colType
      // 
      this.colType.Text = "Type";
      // 
      // colValue
      // 
      this.colValue.Text = "Value";
      // 
      // label1
      // 
      this.label1.Location = new System.Drawing.Point(359, 83);
      this.label1.Name = "label1";
      this.label1.Size = new System.Drawing.Size(180, 44);
      this.label1.TabIndex = 3;
      this.label1.Text = "Inventory";
      this.label1.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
      // 
      // FrmInventory
      // 
      this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
      this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
      this.ClientSize = new System.Drawing.Size(800, 450);
      this.Controls.Add(this.label1);
      this.Controls.Add(this.listViewInventory);
      this.Name = "FrmInventory";
      this.Text = "Form1";
      this.ResumeLayout(false);

    }

    #endregion

    private System.Windows.Forms.ListView listViewInventory;
    private System.Windows.Forms.ColumnHeader colItemName;
    private System.Windows.Forms.ColumnHeader colType;
    private System.Windows.Forms.ColumnHeader colValue;
    private System.Windows.Forms.Label label1;
  }
}
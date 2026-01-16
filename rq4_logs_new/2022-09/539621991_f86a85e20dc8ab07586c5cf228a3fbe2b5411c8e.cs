using System.Windows.Forms;

namespace FactoryMethod
{
    partial class Form1
{
    /// <summary>
    ///  Required designer variable.
    /// </summary>
    private System.ComponentModel.IContainer components = null;

    /// <summary>
    ///  Clean up any resources being used.
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
    ///  Required method for Designer support - do not modify
    ///  the contents of this method with the code editor.
    /// </summary>
    private void InitializeComponent()
    {
        this.components = new System.ComponentModel.Container();
        this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
        this.ClientSize = new System.Drawing.Size(900, 900);
        this.Text = "Form1";

        button1 = new Button();
        button1.Name = "button1";
        button1.Text = "Guess this servant type";
        button1.Location = new System.Drawing.Point(100,20);
        button1.ClientSize = new System.Drawing.Size(100, 50);

        this.Controls.Add(button1);
    }
    private Button button1;
    #endregion
}

}
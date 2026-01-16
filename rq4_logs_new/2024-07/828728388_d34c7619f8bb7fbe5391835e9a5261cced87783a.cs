using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Windows.Forms;

namespace ColorPalette
{
    public class MainForm : Form
    {
        private List<Color> colors = new List<Color>();
        private Button addButton;
        private Button saveButton;
        private Panel colorPanel;
        private ColorDialog colorDialog;

        public MainForm()
        {
            this.Text = "Färgpalett";
            this.Size = new Size(800, 600);
            InitializeComponents();
        }

        private void InitializeComponents()
        {
            addButton = new Button { Text = "Lägg till färg", Location = new Point(10, 10) };
            addButton.Click += AddButton_Click;

            saveButton = new Button { Text = "Spara SVG", Location = new Point(150, 10) };
            saveButton.Click += SaveButton_Click;

            colorPanel = new Panel { Location = new Point(10, 50), Size = new Size(760, 500) };

            this.Controls.Add(addButton);
            this.Controls.Add(saveButton);
            this.Controls.Add(colorPanel);
        }

        private void AddButton_Click(object sender, EventArgs e)
        {
            colorDialog = new ColorDialog();
            if (colorDialog.ShowDialog() == DialogResult.OK)
            {
                colors.Add(colorDialog.Color);
                colorPanel.Invalidate();
            }
        }

        private void SaveButton_Click(object sender, EventArgs e)
        {
            SavePalette();
        }

        protected override void OnPaint(PaintEventArgs e)
        {
            base.OnPaint(e);
            DrawColors(e.Graphics);
        }

        private void DrawColors(Graphics g)
        {
            int width = 100;
            int height = 100;
            int x = 0;
            int y = 0;

            foreach (var color in colors)
            {
                g.FillRectangle(new SolidBrush(color), x, y, width, height);
                g.DrawString($"Hex: #{color.R:X2}{color.G:X2}{color.B:X2}", SystemFonts.DefaultFont, Brushes.Black, x, y + height);
                g.DrawString($"RGB: {color.R}, {color.G}, {color.B}", SystemFonts.DefaultFont, Brushes.Black, x, y + height + 15);
                
                x += width;
                if (x >= colorPanel.Width)
                {
                    x = 0;
                    y += height + 40;
                }
            }
        }

        private void SavePalette()
        {
            using (StreamWriter writer = new StreamWriter("ColorPalette.svg"))
            {
                writer.WriteLine("<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"800\" height=\"600\">");
                int width = 100;
                int height = 100;
                int x = 0;
                int y = 0;

                foreach (var color in colors)
                {
                    writer.WriteLine($"  <rect x=\"{x}\" y=\"{y}\" width=\"{width}\" height=\"{height}\" fill=\"rgb({color.R},{color.G},{color.B})\"/>");
                    writer.WriteLine($"  <text x=\"{x + 5}\" y=\"{y + height - 10}\" fill=\"black\">Hex: #{color.R:X2}{color.G:X2}{color.B:X2}</text>");
                    writer.WriteLine($"  <text x=\"{x + 5}\" y=\"{y + height + 10}\" fill=\"black\">RGB: {color.R}, {color.G}, {color.B}</text>");

                    x += width;
                    if (x >= 800)
                    {
                        x = 0;
                        y += height + 40;
                    }
                }

                writer.WriteLine("</svg>");
                MessageBox.Show("SVG bild sparad som ColorPalette.svg");
            }
        }

        [STAThread]
        static void Main()
        {
            Application.Run(new MainForm());
        }
    }
using System;
using System.Collections;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace WindowsClient.Models
{
    public struct RichText
    {
        public string Text { get; set; }

        public Color Color { get; set; }
    }

    public class Advice
    {
        public bool Empty => Message.Any();

        private List<RichText> Message = new List<RichText>();

        public void Include(string text, Color color) => Message.Add(new RichText { Text = text, Color = color });

        public void AddToTextBox(RichTextBox box)
        {
            box.Clear();

            foreach (var item in Message)
            {
                box.SelectionStart = box.TextLength;
                box.SelectionLength = 0;

                box.SelectionColor = item.Color;
                box.AppendText(item.Text);
                box.SelectionColor = box.ForeColor;
            }
        }

        public void Clear() => Message.Clear();
    }
}
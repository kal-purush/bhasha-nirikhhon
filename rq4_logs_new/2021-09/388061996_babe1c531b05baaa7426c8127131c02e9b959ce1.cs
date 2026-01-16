using System.Windows.Forms;
using System.Drawing;
using SummerTask_RadkevichMarsell.blockScheme.blocks;

namespace SummerTask_RadkevichMarsell.blockScheme
{
    class BlockBlueprint
    {
        Point BlueprintLocation { get; }
        int BlueprintWidth { get; }
        int BlueprintHeight { get; }

        Label TextRectangle { get; }
        BlockTemplate Block { get; }

        BlockBlueprint (BlockTemplate block)
        {
            Block = block;
            TextRectangle = CreateRectangleFromText(Block.Content);
        }

        private Label CreateRectangleFromText(string content)
        {
            var rectangle = new Label();

            rectangle.AutoSize = true;
            rectangle.Enabled = true;
            rectangle.BackColor = Color.White;
            rectangle.Text = content;          

            return rectangle;
        }
    }
}
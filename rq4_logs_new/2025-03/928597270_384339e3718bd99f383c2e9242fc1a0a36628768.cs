using BrawlTCG_alpha.Logic.Cards;
using BrawlTCG_alpha.Visuals;
using System;
using System.Collections.Generic;
using System.Drawing.Drawing2D;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BrawlTCG_alpha.Logic.Managers
{
    internal class PaintCardManager
    {
        // Variables
        const int CARD_WIDTH = 150;
        const int CARD_HEIGHT = 200;
        public Image BackSideImage { get; } = Properties.Resources.BrawlLogo;
        public Font Font { get; } = new Font("Arial", 12, FontStyle.Bold);


        // Paint
        public void PaintCard(PaintEventArgs e, Card card)
        {
            Graphics g = e.Graphics;

            if (card.IsOpen)
            {
                if (card is LegendCard legendCard)
                {
                    PaintLegendCard(g, legendCard);
                }
                else if (card is StageCard stageCard)
                {
                    PaintStageCard(g, stageCard);
                }
                else
                {
                    PaintAnyOtherCard(g, card);
                }
            }
            else
            {
                PaintCardBackSide(g);
            }
            PaintCardBorder(g);
        }

        void PaintLegendCard(Graphics g, Card card)
        {
            LegendCard legendCard = (LegendCard)card;

            Brush brush = new SolidBrush(legendCard.CardColor);
            g.FillRectangle(brush, 0, 0, CARD_WIDTH, CARD_HEIGHT);

            // Define the aspect ratio (4:3 for LegendCard)
            float aspectRatio = 4f / 3f;
            int maxWidth = CARD_WIDTH - 20;
            int maxHeight = CARD_HEIGHT - 60;

            int newWidth = maxWidth;
            int newHeight = (int)(newWidth / aspectRatio);

            // Adjust if the height exceeds the available space
            if (newHeight > maxHeight)
            {
                newHeight = maxHeight;
                newWidth = (int)(newHeight * aspectRatio);
            }

            // Center the image
            int x = 10 + (maxWidth - newWidth) / 2;
            int y = 10 + (maxHeight - newHeight) / 2;

            // Rounded corners for the image
            int cornerRadius = 15; // Adjust for more or less rounding
            GraphicsPath roundedImagePath = new GraphicsPath();
            roundedImagePath.AddArc(x, y, cornerRadius, cornerRadius, 180, 90); // Top-left
            roundedImagePath.AddArc(x + newWidth - cornerRadius, y, cornerRadius, cornerRadius, 270, 90); // Top-right
            roundedImagePath.AddArc(x + newWidth - cornerRadius, y + newHeight - cornerRadius, cornerRadius, cornerRadius, 0, 90); // Bottom-right
            roundedImagePath.AddArc(x, y + newHeight - cornerRadius, cornerRadius, cornerRadius, 90, 90); // Bottom-left
            roundedImagePath.CloseFigure();

            // Clip the drawing area to the rounded rectangle
            g.SetClip(roundedImagePath);

            // Draw the image with rounded corners
            g.DrawImage(legendCard.Image, new Rectangle(x, y, newWidth, newHeight));

            // Reset the clipping region to its default state
            g.ResetClip();

            // Draw text elements
            Brush textBrush = new SolidBrush(card.TextColor);
            g.DrawString(legendCard.Name, Font, textBrush, new PointF(5, 5));
            g.DrawString(legendCard.Cost.ToString(), Font, textBrush, new PointF(CARD_WIDTH - 20, CARD_HEIGHT - 25));
            g.DrawString($"HP {legendCard.CurrentHP}/{legendCard.BaseHealth}", Font, textBrush, new PointF(5, CARD_HEIGHT - 71));
            g.DrawString($"Att {legendCard.Power}", Font, textBrush, new PointF(5, CARD_HEIGHT - 48));
        }

        void PaintStageCard(Graphics g, Card card)
        {
            StageCard stageCard = (StageCard)card;

            Brush brush = new SolidBrush(stageCard.CardColor);
            g.FillRectangle(brush, 0, 0, CARD_WIDTH, CARD_HEIGHT);

            // Define the aspect ratio (4:3 for LegendCard)
            float aspectRatio = 4f / 3f;
            int maxWidth = CARD_WIDTH - 20;
            int maxHeight = CARD_HEIGHT - 60;

            int newWidth = maxWidth;
            int newHeight = (int)(newWidth / aspectRatio);

            // Adjust if the height exceeds the available space
            if (newHeight > maxHeight)
            {
                newHeight = maxHeight;
                newWidth = (int)(newHeight * aspectRatio);
            }

            // Center the image
            int x = 10 + (maxWidth - newWidth) / 2;
            int y = 10 + (maxHeight - newHeight) / 2;

            // Rounded corners for the image
            int cornerRadius = 15; // Adjust for more or less rounding
            GraphicsPath roundedImagePath = new GraphicsPath();
            roundedImagePath.AddArc(x, y, cornerRadius, cornerRadius, 180, 90); // Top-left
            roundedImagePath.AddArc(x + newWidth - cornerRadius, y, cornerRadius, cornerRadius, 270, 90); // Top-right
            roundedImagePath.AddArc(x + newWidth - cornerRadius, y + newHeight - cornerRadius, cornerRadius, cornerRadius, 0, 90); // Bottom-right
            roundedImagePath.AddArc(x, y + newHeight - cornerRadius, cornerRadius, cornerRadius, 90, 90); // Bottom-left
            roundedImagePath.CloseFigure();

            // Clip the drawing area to the rounded rectangle
            g.SetClip(roundedImagePath);

            // Draw the image with rounded corners
            g.DrawImage(stageCard.Image, new Rectangle(x, y, newWidth, newHeight));

            // Reset the clipping region to its default state
            g.ResetClip();

            // Draw text elements
            Brush textBrush = new SolidBrush(card.TextColor);
            g.DrawString(stageCard.Name, Font, textBrush, new PointF(5, 5));
            g.DrawString(stageCard.Cost.ToString(), Font, textBrush, new PointF(CARD_WIDTH - 20, CARD_HEIGHT - 25));
        }

        void PaintAnyOtherCard(Graphics g, Card card)
        {
            Brush cardBrush = new SolidBrush(card.CardColor);
            g.FillRectangle(cardBrush, 0, 0, CARD_WIDTH, CARD_HEIGHT);
            g.DrawImage(card.Image, new Rectangle(10, 30, CARD_WIDTH - 20, CARD_HEIGHT - 60));
            Brush textBrush = new SolidBrush(card.TextColor);
            g.DrawString(card.Name, Font, textBrush, new PointF(5, 5));
            g.DrawString(card.Cost.ToString(), Font, textBrush, new PointF(CARD_WIDTH - 20, CARD_HEIGHT - 25));
        }

        void PaintCardBackSide(Graphics g)
        {
            g.FillRectangle(Brushes.LightBlue, 0, 0, CARD_WIDTH, CARD_HEIGHT);
            g.DrawImage(BackSideImage, new Rectangle(10, 30, CARD_WIDTH - 20, CARD_HEIGHT - 60));
        }

        void PaintCardBorder(Graphics g)
        {
            int borderThickness = 3;
            g.DrawRectangle(new Pen(Color.Black, borderThickness), 0, 0, CARD_WIDTH - 2, CARD_HEIGHT - 2);
        }
    }
}
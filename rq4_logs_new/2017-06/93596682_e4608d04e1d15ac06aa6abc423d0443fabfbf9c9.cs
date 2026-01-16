using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Shapes;

namespace WeatherStation
{
    public class UVIndexGauge : SingleDataGauges
    {
        protected Ellipse backgroundEllipse = new Ellipse();
        protected Polygon valuePointer = new Polygon();

        private SolidColorBrush[] colorArray =
        {
            new SolidColorBrush(Color.FromRgb(142, 198, 63)),//Kleur 1
            new SolidColorBrush(Color.FromRgb(142, 198, 63)),//Kleur 2
            new SolidColorBrush(Color.FromRgb(252, 176, 64)),//Kleur 3
            new SolidColorBrush(Color.FromRgb(252, 176, 64)),//Kleur 4
            new SolidColorBrush(Color.FromRgb(252, 176, 64)),//Kleur 5
            new SolidColorBrush(Color.FromRgb(241, 144, 27)),//Kleur 6
            new SolidColorBrush(Color.FromRgb(241, 144, 27)),//Kleur 7
            new SolidColorBrush(Color.FromRgb(242, 90, 41)),//Kleur 8
            new SolidColorBrush(Color.FromRgb(242, 90, 41)),//Kleur 9
            new SolidColorBrush(Color.FromRgb(242, 90, 41)),//Kleur 10
            new SolidColorBrush(Color.FromRgb(138, 81, 188))//Kleur 11

        };

        public UVIndexGauge(string gaugeName, string unit, SolidColorBrush foreGround, SolidColorBrush backGround, SolidColorBrush gridColor, SolidColorBrush fontColor, int fontSize, FontFamily fontFamily, Canvas parentGrid, double maxValue, double minValue, int gridMajorLinesAmount) : base(gaugeName, unit, foreGround, backGround, gridColor, fontColor, fontSize, fontFamily, parentGrid, maxValue, minValue, gridMajorLinesAmount)
        {
            currentValue = 0;
            Draw();
        }

        public override void Draw()
        {
            backgroundEllipse.Width = parentGrid.Width - topMargin - bottomMargin;
            backgroundEllipse.Height = parentGrid.Height - topMargin - bottomMargin;
            backgroundEllipse.Fill = backGround;
            Canvas.SetTop(backgroundEllipse, topMargin);
            Canvas.SetLeft(backgroundEllipse, topMargin);

            parentGrid.Children.Add(backgroundEllipse);

            for (int i = 1; i <= gridMajorLinesAmount ;  i++)
            {
                Path path = new Path();
                Canvas.SetLeft(path, parentGrid.Width / 2);
                Canvas.SetTop(path, parentGrid.Height / 2);

                path.Fill = colorArray[i - 1];
                path.Stroke = path.Fill;

                PathGeometry pathGeo = new PathGeometry();
                PathFigure pathFig = new PathFigure();
                pathFig.StartPoint = new System.Windows.Point(0, 0);
                pathFig.IsClosed = true;

                LineSegment lSegment = new LineSegment(new System.Windows.Point(Math.Cos(((i - 1) * (360.0 / (gridMajorLinesAmount)) - 180) * Math.PI / 180) * backgroundEllipse.Width / 2, Math.Sin(((i - 1) * (360.0 / (gridMajorLinesAmount)) - 180) * Math.PI / 180) * backgroundEllipse.Width / 2), true);

                ArcSegment arcSegment = new ArcSegment();
                arcSegment.IsLargeArc = false;
                arcSegment.Point = new System.Windows.Point(Math.Cos(((i) * (360.0 / (gridMajorLinesAmount)) - 180) * Math.PI / 180) * backgroundEllipse.Width / 2, Math.Sin(((i) * (360.0 / (gridMajorLinesAmount)) - 180) * Math.PI / 180) * backgroundEllipse.Width / 2);

                arcSegment.Size = new System.Windows.Size(backgroundEllipse.Width / 2, backgroundEllipse.Width / 2);
                arcSegment.SweepDirection = SweepDirection.Clockwise;

                pathFig.Segments.Add(lSegment);
                pathFig.Segments.Add(arcSegment);

                pathGeo.Figures.Add(pathFig);

                path.Data = pathGeo;

                parentGrid.Children.Add(path);

            }
            for (int i = 1; i <= gridMajorLinesAmount; i++)
            {
                Line line = new Line();
                line.StrokeThickness = 2;
                line.Stroke = gridColor;

                line.X1 = parentGrid.Width / 2;
                line.Y1 = parentGrid.Height / 2;

                line.X2 = Math.Cos(((i-1) * (360.0 / (gridMajorLinesAmount)) - 180) * Math.PI / 180) * backgroundEllipse.Width / 2 + topMargin + backgroundEllipse.Width / 2;
                line.Y2 = Math.Sin(((i -1) * (360.0 / (gridMajorLinesAmount)) - 180) * Math.PI / 180) * backgroundEllipse.Width / 2 + topMargin + backgroundEllipse.Width / 2;

                parentGrid.Children.Add(line);
            }

            for(int i = 0; i < gridLabels.Count - 1; i++)
            {
                parentGrid.Children.Add(gridLabels[i]);
                if((i + 1).ToString() == "11")
                {
                    gridLabels[i].Text = (i + 1).ToString() + "+";
                } else
                {
                    gridLabels[i].Text = (i + 1).ToString();

                }

                gridLabels[i].FontFamily = fontFamilyGauge;
                gridLabels[i].FontSize = fontSize;
                gridLabels[i].Foreground = new SolidColorBrush(Colors.White);

                Canvas.SetTop(gridLabels[i], Math.Sin(((i) * (360.0 / (gridMajorLinesAmount)) - 180 + (360.0 / (gridMajorLinesAmount) / 2)) * Math.PI / 180) * (backgroundEllipse.Width / 2 - 0.1*backgroundEllipse.Width) + topMargin + backgroundEllipse.Width / 2 - 4 );
                Canvas.SetLeft(gridLabels[i], Math.Cos(((i) * (360.0 / (gridMajorLinesAmount)) - 180 + (360.0 / (gridMajorLinesAmount) / 2)) * Math.PI / 180) * (backgroundEllipse.Width / 2 - 0.1 * backgroundEllipse.Width) + topMargin + backgroundEllipse.Width / 2 - 4);

            }

            Ellipse ellOverlay = new Ellipse();
            ellOverlay.Fill = backGround;
            ellOverlay.Width = 0.45* backgroundEllipse.Width;
            ellOverlay.Height = 0.45 * backgroundEllipse.Height;
            Canvas.SetTop(ellOverlay, parentGrid.Height / 2 - ellOverlay.Height / 2);
            Canvas.SetLeft(ellOverlay, parentGrid.Width / 2 - ellOverlay.Width / 2);
            parentGrid.Children.Add(ellOverlay);

            gaugeTitleTextBlock.Text = name;
            gaugeTitleTextBlock.FontFamily = fontFamilyGauge;
            gaugeTitleTextBlock.FontSize = fontSize;
            gaugeTitleTextBlock.Foreground = fontColor;
            gaugeTitleTextBlock.Width = parentGrid.Width;
            gaugeTitleTextBlock.TextAlignment = System.Windows.TextAlignment.Center;

            parentGrid.Children.Add(gaugeTitleTextBlock);

            valuePointer.Fill = foreGround;
            valuePointer.Stroke = foreGround;
            valuePointer.StrokeThickness = 4;
            valuePointer.Points.Add(new System.Windows.Point(parentGrid.Width/2, parentGrid.Height/2));

            valuePointer.Points.Add(new System.Windows.Point(Math.Cos((currentValue * (360/gridMajorLinesAmount) - 180) * Math.PI/180 ) * backgroundEllipse.Width/2 * 0.5 + parentGrid.Width/2 , Math.Sin((currentValue * (360 / gridMajorLinesAmount) - 180 ) * Math.PI / 180) * backgroundEllipse.Width / 2 * 0.5 + parentGrid.Width / 2));

            parentGrid.Children.Add(valuePointer);


        }

        public override void Update()
        {
            valuePointer.Points.Clear();
            valuePointer.Points.Add(new System.Windows.Point(parentGrid.Width / 2, parentGrid.Height / 2));

            if(currentValue != 0)
            {
                valuePointer.Points.Add(new System.Windows.Point(Math.Cos((currentValue * (360 / gridMajorLinesAmount) - 180 - (360 / gridMajorLinesAmount) / 2) * Math.PI / 180) * backgroundEllipse.Width / 2 * 0.5 + parentGrid.Width / 2, Math.Sin((currentValue * (360 / gridMajorLinesAmount) - 180 - (360 / gridMajorLinesAmount) / 2) * Math.PI / 180) * backgroundEllipse.Width / 2 * 0.5 + parentGrid.Width / 2));
            } else
            {
                valuePointer.Points.Add(new System.Windows.Point(Math.Cos((currentValue * (360 / gridMajorLinesAmount) - 180) * Math.PI / 180) * backgroundEllipse.Width / 2 * 0.5 + parentGrid.Width / 2, Math.Sin((currentValue * (360 / gridMajorLinesAmount) - 180) * Math.PI / 180) * backgroundEllipse.Width / 2 * 0.5 + parentGrid.Width / 2));
            }
            
        }
    }
}
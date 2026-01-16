using System;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Shapes;
using System.Windows.Media;
using System.IO;


namespace DotsDotsDots
{
    public class MyWindow : Window
    {
        Grid myGrid = new Grid();
        Brush fillColor = System.Windows.Media.Brushes.Azure;
        Brush BGBrush = System.Windows.Media.Brushes.LightYellow;
        private Label label1=new Label();
        
        ComboBox fillComboBox = new ComboBox();
        ComboBox colorComboBox = new ComboBox();
        PointCollection points = new PointCollection();
        public MyWindow()
        {
            Width = 1536;
            Height = 768;
            Title = "My window";
            Background = BGBrush;
            int MoveX = 200;
            int MoveY = 200;
            Point bufferPoint = new Point();
            
            Content = myGrid;
            //Считываю строку с точками из файла
            //PointCollection readPoints = PointCollection.Parse(ReadPoints()); 

            //Так как строку с точками мы больше не считываем из файла, строка будет храниться в таком виде, сразу считаем количество
            PointCollection readPoints = PointCollection.Parse("20,20 846,20 868,40 42,40  912,40 934,60 956,80 868,80 846,60 64,60  86,80  980,80 1002,100 1024,120 868,120 846,100 108,100  130,120 920,120 894,140  152,140 868,160 460,160 438,140");
             int pCount = readPoints.Count();

            //Этот цикл тут для того, чтобы можно было двигать всю структуру.
            for (int i = 0; i < pCount;)
            {
                bufferPoint.X = readPoints[i].X + MoveX;
                bufferPoint.Y = readPoints[i].Y + MoveY;
                points.Add(bufferPoint);
                i++;
            }

            fillComboBox.HorizontalAlignment = HorizontalAlignment.Left;
            fillComboBox.VerticalAlignment = VerticalAlignment.Top;
            colorComboBox.HorizontalAlignment = HorizontalAlignment.Left;
            colorComboBox.VerticalAlignment = VerticalAlignment.Top;

            //Быстрый способ заполнить комбобоксы, сразу зададим выбор по умолчанию
            for (int i = 0; i < 9; i++){colorComboBox.Items.Add("Цвет " + (i + 1));}
            colorComboBox.SelectedIndex = 0;
            colorComboBox.Margin = new Thickness(100, 0, 0, 0);

            for (int i = 0; i < 9; i++){fillComboBox.Items.Add("Заливка " + (i + 1));}
            fillComboBox.SelectedIndex = 0;

            //Просто текст. Отображается текст из комбобокса с вариантом заливки.
            label1.Content = fillComboBox.Text;
            label1.FontSize = 20;
            label1.Foreground= System.Windows.Media.Brushes.Black;
            label1.Margin = new Thickness(550 + MoveX, 60 + MoveY, 0, 0);
            
            //Рисуем всё
            myGrid.Children.Add(fillComboBox);
            myGrid.Children.Add(colorComboBox);
            myGrid.Children.Add(label1);
            DrawLines();
            DrawDots();

            //Обрабатываем события при изменении выбранного пункта комбобоксов
            fillComboBox.SelectionChanged += new SelectionChangedEventHandler(Event_Fill);
            colorComboBox.SelectionChanged += new SelectionChangedEventHandler(Event_Select_Color);


        }

        //Рисует линию по двум точкам. Толщина и цвет фиксированы.
        public void DrawLine(Point p1, Point p2)
        {
            Line myLine = new Line();
            myLine.Stroke = System.Windows.Media.Brushes.Blue;
            myLine.X1 = p1.X;
            myLine.X2 = p2.X;
            myLine.Y1 = p1.Y;
            myLine.Y2 = p2.Y;
            myLine.HorizontalAlignment = HorizontalAlignment.Left;
            myLine.VerticalAlignment = VerticalAlignment.Top;
            myLine.StrokeThickness = 1;
            myGrid.Children.Add(myLine);
        }
        //Просто рисует все линии по точкам
        void DrawLines()
        {
            //Первый блок
            DrawLine(points[0], points[1]);
            DrawLine(points[1], points[2]);
            DrawLine(points[2], points[3]);
            DrawLine(points[3], points[0]);

            //Второй блок
            DrawLine(points[2], points[4]);
            DrawLine(points[4], points[5]);
            DrawLine(points[5], points[6]);
            DrawLine(points[6], points[7]);
            DrawLine(points[7], points[8]);
            DrawLine(points[8], points[9]);
            DrawLine(points[9], points[3]);

            //Третий блок
            DrawLine(points[7], points[10]);
            DrawLine(points[10], points[9]);

            //Четвёртый блок
            DrawLine(points[6], points[11]);
            DrawLine(points[11], points[12]);
            DrawLine(points[12], points[13]);
            DrawLine(points[13], points[14]);
            DrawLine(points[14], points[15]);
            DrawLine(points[15], points[16]);
            DrawLine(points[16], points[10]);
            //Пятый блок
            DrawLine(points[14], points[17]);
            DrawLine(points[17], points[16]);
            //Шестой блок
            DrawLine(points[18], points[19]);
            DrawLine(points[19], points[20]);
            DrawLine(points[20], points[17]);
            //Седьмой блок
            DrawLine(points[19], points[21]);
            DrawLine(points[21], points[22]);
            DrawLine(points[22], points[23]);
        }
        //Рисует залитый круг с центром в p. d - его диаметр.
        public void DrawPoint(Point p, int d)
        {
            Ellipse myCircle = new Ellipse();
            myCircle.Stroke = System.Windows.Media.Brushes.Red;
            myCircle.HorizontalAlignment = HorizontalAlignment.Left;
            myCircle.VerticalAlignment = VerticalAlignment.Top;
            myCircle.StrokeThickness = 0;
            myCircle.Fill = System.Windows.Media.Brushes.Red;
            myCircle.Width = d;
            myCircle.Height = d;
            myCircle.Margin = new Thickness(p.X - (d / 2), p.Y - (d / 2), 0, 0);
            myGrid.Children.Add(myCircle);
        }
        //Рисует кружки маленького диаметра вокруг нужных точек
        void DrawDots()
        {
            //Первые точки
            DrawPoint(points[0], 5);
            DrawPoint(points[2], 5);
            DrawPoint(points[3], 5);
            //Вторые точки
            DrawPoint(points[6], 5);
            DrawPoint(points[7], 5);
            DrawPoint(points[9], 5);
            //Третьи точки
            DrawPoint(points[10], 5);
            //Четвёртые точки
            DrawPoint(points[13], 5);
            DrawPoint(points[14], 5);
            DrawPoint(points[16], 5);
            //Пятые(?) точки
            DrawPoint(points[17], 5);
            //Шестые точки
            DrawPoint(points[18], 5);
            DrawPoint(points[19], 5);
            DrawPoint(points[20], 5);
            //Седьмые точки
            DrawPoint(points[21], 5);
            DrawPoint(points[23], 5);
        }

        //Функция считывает координаты точек из файла.
        /*static string ReadPoints()
        {
            TextReader tr = new StreamReader("TextFile1.txt");
            string pString = tr.ReadLine();
            tr.Close();
            return (pString);
        }*/

        //Так как произвольный многоугольник можно разбить на треугольники, заливать фигуру мы будем именно ими. Эта функция рисует залитый треугольник.
        public void FillTriangle(Point p1, Point p2, Point p3, Brush fillColor)
        {
            Polygon myTriangle = new Polygon();
            myTriangle.Stroke = fillColor;
            myTriangle.Fill = fillColor;
            myTriangle.HorizontalAlignment = HorizontalAlignment.Left;
            myTriangle.VerticalAlignment = VerticalAlignment.Top;
            myTriangle.StrokeThickness = 1;
            PointCollection myPointCollection = new PointCollection();
            myPointCollection.Add(p1);
            myPointCollection.Add(p2);
            myPointCollection.Add(p3);
            myTriangle.Points = myPointCollection;
            myGrid.Children.Add(myTriangle);

        }

        //В задаче фигурируют только фигуры, которые легко разбиваются на выпуклые четырёхугольники. Заливаем выпуклый четырёхугольник.
        public void FillRectangle(Point p1, Point p2, Point p3, Point p4, Brush fillColor)
        {
            FillTriangle(p1, p3, p2, fillColor);
            FillTriangle(p1, p3, p4, fillColor);
        }

        //Варианты заполнения, которые можно комбинировать.
        void Fill1(PointCollection points, Brush fillColor)
        { FillRectangle(points[0], points[1], points[2], points[3], fillColor); }
        void Fill2(PointCollection points, Brush fillColor)
        {
            FillRectangle(points[3], points[4], points[5], points[9], fillColor);
            FillRectangle(points[8], points[5], points[6], points[7], fillColor);
        }
        void Fill3(PointCollection points, Brush fillColor)
        { FillRectangle(points[9], points[8], points[7], points[10], fillColor); }
        void Fill4(PointCollection points, Brush fillColor)
        {
            FillRectangle(points[10], points[11], points[12], points[16], fillColor);
            FillRectangle(points[15], points[12], points[13], points[14], fillColor);
        }
        void Fill5(PointCollection points, Brush fillColor)
        { FillRectangle(points[16], points[15], points[14], points[17], fillColor); }
        void Fill6(PointCollection points, Brush fillColor)
        { FillRectangle(points[17], points[18], points[19], points[20], fillColor); }
        void Fill7(PointCollection points, Brush fillColor)
        { FillRectangle(points[23], points[19], points[21], points[22], fillColor); }

        //Меняет цвет кисти и вызывает Event_Fill для перерисовки
        void Event_Select_Color(object sender, SelectionChangedEventArgs e)
        {
            switch (colorComboBox.SelectedIndex)
            {
                case 0:
                    fillColor = System.Windows.Media.Brushes.Azure;
                    break;
                case 1:
                    fillColor = System.Windows.Media.Brushes.DeepPink;
                    break;
                case 2:
                    fillColor = System.Windows.Media.Brushes.AliceBlue;
                    break;
                case 3:
                    fillColor = System.Windows.Media.Brushes.Bisque;
                    break;
                case 4:
                    fillColor = System.Windows.Media.Brushes.Crimson;
                    break;
                case 5:
                    fillColor = System.Windows.Media.Brushes.Fuchsia;
                    break;
                case 6:
                    fillColor = System.Windows.Media.Brushes.Olive;
                    break;
                case 7:
                    fillColor = System.Windows.Media.Brushes.DarkGreen;
                    break;
                case 8:
                    fillColor = System.Windows.Media.Brushes.Tan;
                    break;
            }
            Event_Fill(sender, e);
        }


        /*  Автоматически вызывается при изменении списка с вариантами заполнения. Также вызывается вручную при изменении списка с выбором цвета.
         *  Функция очищает и перерисовывает окно. 
         */
        void Event_Fill (object sender, SelectionChangedEventArgs e)
        {
            myGrid.Children.Clear();
            myGrid.Children.Add(fillComboBox);
            myGrid.Children.Add(colorComboBox);
            switch (fillComboBox.SelectedIndex)
            {
                case 0:
                    Fill1(points, BGBrush);
                    Fill2(points, BGBrush);
                    Fill3(points, BGBrush);
                    Fill4(points, BGBrush);
                    Fill5(points, BGBrush);
                    Fill6(points, BGBrush);
                    Fill7(points, BGBrush);
                    break;
                case 1:
                    Fill1(points, fillColor);
                    Fill2(points, BGBrush);
                    Fill3(points, BGBrush);
                    Fill4(points, BGBrush);
                    Fill5(points, BGBrush);
                    Fill6(points, BGBrush);
                    Fill7(points, BGBrush);
                    break;
                case 2:
                    Fill1(points, BGBrush);
                    Fill2(points, fillColor);
                    Fill3(points, BGBrush);
                    Fill4(points, BGBrush);
                    Fill5(points, BGBrush);
                    Fill6(points, BGBrush);
                    Fill7(points, BGBrush);
                    break;
                case 3:
                    Fill1(points, BGBrush);
                    Fill2(points, BGBrush);
                    Fill3(points, fillColor);
                    Fill4(points, BGBrush);
                    Fill5(points, BGBrush);
                    Fill6(points, BGBrush);
                    Fill7(points, BGBrush);
                    break;
                case 4:
                    Fill1(points, BGBrush);
                    Fill2(points, BGBrush);
                    Fill3(points, BGBrush);
                    Fill4(points, fillColor);
                    Fill5(points, BGBrush);
                    Fill6(points, BGBrush);
                    Fill7(points, BGBrush);
                    break;
                case 5:
                    Fill1(points, BGBrush);
                    Fill2(points, BGBrush);
                    Fill3(points, BGBrush);
                    Fill4(points, BGBrush);
                    Fill5(points, fillColor);
                    Fill6(points, BGBrush);
                    Fill7(points, BGBrush);
                    break;
                case 6:
                    Fill1(points, BGBrush);
                    Fill2(points, BGBrush);
                    Fill3(points, BGBrush);
                    Fill4(points, BGBrush);
                    Fill5(points, BGBrush);
                    Fill6(points, fillColor);
                    Fill7(points, BGBrush);
                    break;
                case 7:
                    Fill1(points, BGBrush);
                    Fill2(points, BGBrush);
                    Fill3(points, BGBrush);
                    Fill4(points, BGBrush);
                    Fill5(points, BGBrush);
                    Fill6(points, BGBrush);
                    Fill7(points, fillColor);
                    break;
                case 8:
                    Fill1(points, fillColor);
                    Fill2(points, fillColor);
                    Fill3(points, fillColor);
                    Fill4(points, fillColor);
                    Fill5(points, fillColor);
                    Fill6(points, fillColor);
                    Fill7(points, fillColor);
                    break;
            }
            label1.Content = fillComboBox.SelectedItem;
            myGrid.Children.Add(label1);
            DrawLines();
            DrawDots();
        }

        [STAThread]
        public static void Main()
        {
            Application app = new Application();

            app.Run(new MyWindow());
        }
    }
}
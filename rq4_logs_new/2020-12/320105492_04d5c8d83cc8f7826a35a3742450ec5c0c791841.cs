using System;
using System.Collections.Generic;
using System.Data.Linq;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using LiveCharts;
using LiveCharts.Wpf;
using Brushes = System.Windows.Media.Brushes;

using NozzleLib;

namespace SimuladorNozzle
{
    /// <summary>
    /// Lógica de interacción para MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        List<int> posChart = new List<int>();                         // Select positions to show on the chart

        Nozzle nozzlesim = new Nozzle(3, 800, 0.5, 0.5, 31);                   //Nozzle where we would simulate
        public MainWindow()
        {
            InitializeComponent();
            CreateIndicator(400);
            PropertiesBoxSelection.Items.Add("Temperature");
            PropertiesBoxSelection.Items.Add("Velocity");
            PropertiesBoxSelection.Items.Add("Density");
            PropertiesBoxSelection.Items.Add("Pressure");
            // fill posChart of zeros
            fillSelectedList();

            SetChartV();
        }

        //INITIAL SETTINGS
        private void DefaultValuesButton_Click(object sender, RoutedEventArgs e)
        {
            DivisionsTextBox.Text = "31";
            CourantTextBox.Text = "0.5";
        }

        private void CreateButton_Click(object sender, RoutedEventArgs e)
        {
            PropertiesBoxSelection.SelectedIndex = 0;

            double C = Convert.ToDouble(CourantTextBox.Text);
            int divisions = Convert.ToInt32(DivisionsTextBox.Text);
            nozzlesim = new Nozzle(3, 800, 0.5, C, divisions);
            CreateNozzle(nozzlesim, 0);

        }

        //CONTROLS SIMULATOR
        private void PropertiesBoxSelection_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            CreateNozzle(nozzlesim, 0);             //**de moment 0 el temps, pero ja veurem quan tingui un timestep diferent
        }

        //CONTROLS CHARTS
        private void buttChart0_Click(object sender, RoutedEventArgs e)
        {
            Button button = buttChart0;
            // 0 doesnt shows, 1 shows
            int pos = Convert.ToInt32(button.Content.ToString().Split(' ')[2]);
            int showed = posChart[pos];
            if (showed == 0)
            {
                posChart[pos] = 1;
                Color colorset = Color.FromRgb(153, 144, 144);
                Brush colorBrush = new SolidColorBrush(colorset);
                button.Background = colorBrush;
            }
            else
            {
                posChart[pos] = 0;
                Color colorset = Color.FromRgb(232, 232, 232);
                Brush colorBrush = new SolidColorBrush(colorset);
                button.Background = colorBrush;
            }
            SetChartV();
        }
        private void buttChart1_Click(object sender, RoutedEventArgs e)
        {
            Button button = buttChart1;
            // 0 doesnt shows, 1 shows
            int pos = Convert.ToInt32(button.Content.ToString().Split(' ')[2]);
            int showed = posChart[pos];
            if (showed == 0)
            {
                posChart[pos] = 1;
                Color colorset = Color.FromRgb(153, 144, 144);
                Brush colorBrush = new SolidColorBrush(colorset);
                button.Background = colorBrush;
            }
            else
            {
                posChart[pos] = 0;
                Color colorset = Color.FromRgb(232, 232, 232);
                Brush colorBrush = new SolidColorBrush(colorset);
                button.Background = colorBrush;
            }
            SetChartV();
        }

        private void buttChart2_Click(object sender, RoutedEventArgs e)
        {
            Button button = buttChart2;
            // 0 doesnt shows, 1 shows
            int pos = Convert.ToInt32(button.Content.ToString().Split(' ')[2]);
            int showed = posChart[pos];
            if (showed == 0)
            {
                posChart[pos] = 1;
                Color colorset = Color.FromRgb(153, 144, 144);
                Brush colorBrush = new SolidColorBrush(colorset);
                button.Background = colorBrush;
            }
            else
            {
                posChart[pos] = 0;
                Color colorset = Color.FromRgb(232, 232, 232);
                Brush colorBrush = new SolidColorBrush(colorset);
                button.Background = colorBrush;
            }
            SetChartV();
        }

        private void buttChart3_Click(object sender, RoutedEventArgs e)
        {
            Button button = buttChart3;
            // 0 doesnt shows, 1 shows
            int pos = Convert.ToInt32(button.Content.ToString().Split(' ')[2]);
            int showed = posChart[pos];
            if (showed == 0)
            {
                posChart[pos] = 1;
                Color colorset = Color.FromRgb(153, 144, 144);
                Brush colorBrush = new SolidColorBrush(colorset);
                button.Background = colorBrush;
            }
            else
            {
                posChart[pos] = 0;
                Color colorset = Color.FromRgb(232, 232, 232);
                Brush colorBrush = new SolidColorBrush(colorset);
                button.Background = colorBrush;
            }
            SetChartV();
        }

        //FUNCTIONS SIMULATOR
        private void CreateIndicator(int filas)
        {
            int change = Convert.ToInt32(filas / 20);

            int count = 0;
            while (count < filas)
            {
                Indicator.RowDefinitions.Add(new RowDefinition());
                count++;
            }

            int i = 0;
            foreach (RowDefinition row in Indicator.RowDefinitions)
            {
                StackPanel panel = new StackPanel();
                byte A;
                byte R;
                byte G;
                byte B;
                if (i < change)
                {
                    A = 255;
                    double r = 0 + i * 128 / change;
                    R = Convert.ToByte(Math.Round(r, 0));
                    G = 0;
                    double b = 0 + i * 32 / change;
                    B = Convert.ToByte(Math.Round(b, 0));

                }
                else if (i < 2 * change)
                {
                    int j = i - change;
                    A = 255;
                    R = 128;
                    double g = 0 + j * 22 / change;
                    G = Convert.ToByte(Math.Round(g, 0));
                    double b = 32 + j * 24 / change;
                    B = Convert.ToByte(Math.Round(b, 0));

                }
                else if (i < 3 * change)
                {
                    int j = i - 2 * change;
                    A = 255;
                    double r = 128 + j * 36 / change;
                    R = Convert.ToByte(Math.Round(r, 0));
                    double g = 22;
                    G = Convert.ToByte(Math.Round(g, 0));
                    double b = 56 + j * 16 / change;
                    B = Convert.ToByte(Math.Round(b, 0));
                }
                else if (i < 4 * change)
                {
                    int j = i - 3 * change;
                    A = 255;
                    double r = 164 + j * 87 / change;
                    R = Convert.ToByte(Math.Round(r, 0));
                    double g = 22 + j * 9 / change;
                    G = Convert.ToByte(Math.Round(g, 0));
                    double b = 72 - j * 42 / change;
                    B = Convert.ToByte(Math.Round(b, 0));
                }
                else if (i < 5 * change)
                {
                    int j = i - 4 * change;
                    A = 255;
                    double r = 251;
                    R = Convert.ToByte(Math.Round(r, 0));
                    double g = 31 + j * 67 / change;
                    G = Convert.ToByte(Math.Round(g, 0));
                    double b = 30;
                    B = Convert.ToByte(Math.Round(b, 0));
                }
                else if (i < 6 * change)
                {
                    int j = i - 5 * change;
                    A = 255;
                    double r = 251 - j * 13 / change;
                    R = Convert.ToByte(Math.Round(r, 0));
                    double g = 98 + j * 21 / change;
                    G = Convert.ToByte(Math.Round(g, 0));
                    double b = 30 + j * 38 / change;
                    B = Convert.ToByte(Math.Round(b, 0));
                }
                else if (i < 7 * change)
                {
                    int j = i - 6 * change;
                    A = 255;
                    double r = 238 + j * 17 / change;
                    R = Convert.ToByte(Math.Round(r, 0));
                    double g = 119 + j * 130 / change;
                    G = Convert.ToByte(Math.Round(g, 0));
                    double b = 68 - j * 22 / change;
                    B = Convert.ToByte(Math.Round(b, 0));
                }
                else if (i < 8 * change)
                {
                    int j = i - 7 * change;
                    A = 255;
                    double r = 255 - j * 63 / change;
                    R = Convert.ToByte(Math.Round(r, 0));
                    double g = 249 + j * 6 / change;
                    G = Convert.ToByte(Math.Round(g, 0));
                    double b = 46 - j * 46 / change;
                    B = Convert.ToByte(Math.Round(b, 0));
                }
                else if (i < 9 * change)
                {
                    int j = i - 8 * change;
                    A = 255;
                    double r = 192 - j * 192 / change;
                    R = Convert.ToByte(Math.Round(r, 0));
                    double g = 255 - j * 71 / change;
                    G = Convert.ToByte(Math.Round(g, 0));
                    double b = 0;
                    B = Convert.ToByte(Math.Round(b, 0));
                }
                else if (i < 10 * change)
                {
                    int j = i - 9 * change;
                    A = 255;
                    double r = 0 + j * 25 / change;
                    R = Convert.ToByte(Math.Round(r, 0));
                    double g = 184 + j * 35 / change;
                    G = Convert.ToByte(Math.Round(g, 0));
                    double b = 0 + j * 89 / change;
                    B = Convert.ToByte(Math.Round(b, 0));
                }
                else if (i < 11 * change)
                {
                    int j = i - 10 * change;
                    A = 255;
                    double r = 25 + j * 26 / change;
                    R = Convert.ToByte(Math.Round(r, 0));
                    double g = 219 + j * 36 / change;
                    G = Convert.ToByte(Math.Round(g, 0));
                    double b = 89 + j * 89 / change;
                    B = Convert.ToByte(Math.Round(b, 0));
                }
                else if (i < 12 * change)
                {
                    int j = i - 11 * change;
                    A = 255;
                    double r = 51 - j * 51 / change;
                    R = Convert.ToByte(Math.Round(r, 0));
                    double g = 255 - j * 45 / change;
                    G = Convert.ToByte(Math.Round(g, 0));
                    double b = 178 + j * 32 / change;
                    B = Convert.ToByte(Math.Round(b, 0));
                }
                else if (i < 13 * change)
                {
                    int j = i - 12 * change;
                    A = 255;
                    double r = 0 + j * 15 / change;
                    R = Convert.ToByte(Math.Round(r, 0));
                    double g = 210 - j * 83 / change;
                    G = Convert.ToByte(Math.Round(g, 0));
                    double b = 210 - j * 83 / change;
                    B = Convert.ToByte(Math.Round(b, 0));
                }
                else if (i < 14 * change)
                {
                    int j = i - 13 * change;
                    A = 255;
                    double r = 15 + j * 6 / change;
                    R = Convert.ToByte(Math.Round(r, 0));
                    double g = 127 - j * 31 / change;
                    G = Convert.ToByte(Math.Round(g, 0));
                    double b = 127 + j * 62 / change;
                    B = Convert.ToByte(Math.Round(b, 0));
                }
                else if (i < 15 * change)
                {
                    int j = i - 14 * change;
                    A = 255;
                    double r = 21 - j * 10 / change;
                    R = Convert.ToByte(Math.Round(r, 0));
                    double g = 96 - j * 38 / change;
                    G = Convert.ToByte(Math.Round(g, 0));
                    double b = 189 - j * 31 / change;
                    B = Convert.ToByte(Math.Round(b, 0));
                }
                else if (i < 16 * change)
                {
                    int j = i - 15 * change;
                    A = 255;
                    double r = 11 - j * 11 / change;
                    R = Convert.ToByte(Math.Round(r, 0));
                    double g = 58 - j * 38 / change;
                    G = Convert.ToByte(Math.Round(g, 0));
                    double b = 158 - j * 31 / change;
                    B = Convert.ToByte(Math.Round(b, 0));
                }
                else if (i < 17 * change)
                {
                    int j = i - 16 * change;
                    A = 255;
                    double r = 0 + j * 65 / change;
                    R = Convert.ToByte(Math.Round(r, 0));
                    double g = 20 + j * 8 / change;
                    G = Convert.ToByte(Math.Round(g, 0));
                    double b = 127 + j * 60 / change;
                    B = Convert.ToByte(Math.Round(b, 0));
                }
                else if (i < 18 * change)
                {
                    int j = i - 17 * change;
                    A = 255;
                    double r = 65 + j * 84 / change;
                    R = Convert.ToByte(Math.Round(r, 0));
                    double g = 28 + j * 15 / change;
                    G = Convert.ToByte(Math.Round(g, 0));
                    double b = 187 - j * 33 / change;
                    B = Convert.ToByte(Math.Round(b, 0));
                }
                else if (i < 19 * change)
                {
                    int j = i - 18 * change;
                    A = 255;
                    double r = 149 + j * 52 / change;
                    R = Convert.ToByte(Math.Round(r, 0));
                    double g = 43 + j * 117 / change;
                    G = Convert.ToByte(Math.Round(g, 0));
                    double b = 154 + j * 66 / change;
                    B = Convert.ToByte(Math.Round(b, 0));
                }
                else
                {
                    int j = i - 19 * change;
                    A = 255;
                    double r = 201 + j * 18 / change;
                    R = Convert.ToByte(Math.Round(r, 0));
                    double g = 160 + j * 52 / change;
                    G = Convert.ToByte(Math.Round(g, 0));
                    double b = 220 + j * 35 / change;
                    B = Convert.ToByte(Math.Round(b, 0));
                }
                Color colorset = Color.FromArgb(A, R, G, B);
                panel.Background = new SolidColorBrush(colorset);
                Grid.SetRow(panel, i);
                Indicator.Children.Add(panel);
                i++;
            }
        }

        private void CreateNozzle(Nozzle nozzle, int t)
        {
            NozzleCanvas.Children.RemoveRange(0, NozzleCanvas.Children.Count);
            double width = 445 / (nozzle.GetDivisions());
            int count = 0;
            while (count < nozzle.GetDivisions())
            {
                int propind = PropertiesBoxSelection.SelectedIndex;
                double value = 0;
                if (propind == 0)
                {
                    value = nozzle.GetPosition(t, count).GetTemperature();
                }
                else if (propind == 1)
                {
                    value = nozzle.GetPosition(t, count).GetVelocity();
                }
                else if (propind == 2)
                {
                    value = nozzle.GetPosition(t, count).GetDensity();
                }
                else if (propind == 3)
                {
                    value = nozzle.GetPosition(t, count).GetPressure();
                }
                Color color = PrintColor(propind, value);
                Rectangle rect = new Rectangle();
                rect.Stroke = new SolidColorBrush(color);
                rect.Fill = new SolidColorBrush(color);
                rect.StrokeThickness = 2;
                rect.Width = width;
                rect.Height = 100 + (nozzle.GetPosition(0, count).GetArea() - 1) * 200 / 4.95;
                Canvas.SetLeft(rect, 5 + width * count);
                Canvas.SetTop(rect, 150 - (rect.Height - 100) * 100 / 200);
                NozzleCanvas.Children.Add(rect);
                count++;
            }

        }

        private Color PrintColor(int propind, double i)
        {
            double max = 1;
            double min = 0;
            if (propind == 0)
            {
                max = 1;
                min = 0.3;
            }
            else if (propind == 1)
            {
                max = 2;
                min = 0;
            }
            else if (propind == 2)
            {
                max = 1;
                min = 0.04;
            }
            else if (propind == 3)
            {
                max = 1;
                min = 0.005;
            }
            byte A = 255;
            byte R;
            byte G;
            byte B;

            i = max - i;
            double change = (max - min) / 20;

            if (i < change)
            {
                A = 255;
                double r = 0 + i * 128 / change;
                R = Convert.ToByte(Math.Round(r, 0));
                G = 0;
                double b = 0 + i * 32 / change;
                B = Convert.ToByte(Math.Round(b, 0));

            }
            else if (i < 2 * change)
            {
                double j = i - change;
                A = 255;
                R = 128;
                double g = 0 + j * 22 / change;
                G = Convert.ToByte(Math.Round(g, 0));
                double b = 32 + j * 24 / change;
                B = Convert.ToByte(Math.Round(b, 0));

            }
            else if (i < 3 * change)
            {
                double j = i - 2 * change;
                A = 255;
                double r = 128 + j * 36 / change;
                R = Convert.ToByte(Math.Round(r, 0));
                double g = 22;
                G = Convert.ToByte(Math.Round(g, 0));
                double b = 56 + j * 16 / change;
                B = Convert.ToByte(Math.Round(b, 0));
            }
            else if (i < 4 * change)
            {
                double j = i - 3 * change;
                A = 255;
                double r = 164 + j * 87 / change;
                R = Convert.ToByte(Math.Round(r, 0));
                double g = 22 + j * 9 / change;
                G = Convert.ToByte(Math.Round(g, 0));
                double b = 72 - j * 42 / change;
                B = Convert.ToByte(Math.Round(b, 0));
            }
            else if (i < 5 * change)
            {
                double j = i - 4 * change;
                A = 255;
                double r = 251;
                R = Convert.ToByte(Math.Round(r, 0));
                double g = 31 + j * 67 / change;
                G = Convert.ToByte(Math.Round(g, 0));
                double b = 30;
                B = Convert.ToByte(Math.Round(b, 0));
            }
            else if (i < 6 * change)
            {
                double j = i - 5 * change;
                A = 255;
                double r = 251 - j * 13 / change;
                R = Convert.ToByte(Math.Round(r, 0));
                double g = 98 + j * 21 / change;
                G = Convert.ToByte(Math.Round(g, 0));
                double b = 30 + j * 38 / change;
                B = Convert.ToByte(Math.Round(b, 0));
            }
            else if (i < 7 * change)
            {
                double j = i - 6 * change;
                A = 255;
                double r = 238 + j * 17 / change;
                R = Convert.ToByte(Math.Round(r, 0));
                double g = 119 + j * 130 / change;
                G = Convert.ToByte(Math.Round(g, 0));
                double b = 68 - j * 22 / change;
                B = Convert.ToByte(Math.Round(b, 0));
            }
            else if (i < 8 * change)
            {
                double j = i - 7 * change;
                A = 255;
                double r = 255 - j * 63 / change;
                R = Convert.ToByte(Math.Round(r, 0));
                double g = 249 + j * 6 / change;
                G = Convert.ToByte(Math.Round(g, 0));
                double b = 46 - j * 46 / change;
                B = Convert.ToByte(Math.Round(b, 0));
            }
            else if (i < 9 * change)
            {
                double j = i - 8 * change;
                A = 255;
                double r = 192 - j * 192 / change;
                R = Convert.ToByte(Math.Round(r, 0));
                double g = 255 - j * 71 / change;
                G = Convert.ToByte(Math.Round(g, 0));
                double b = 0;
                B = Convert.ToByte(Math.Round(b, 0));
            }
            else if (i < 10 * change)
            {
                double j = i - 9 * change;
                A = 255;
                double r = 0 + j * 25 / change;
                R = Convert.ToByte(Math.Round(r, 0));
                double g = 184 + j * 35 / change;
                G = Convert.ToByte(Math.Round(g, 0));
                double b = 0 + j * 89 / change;
                B = Convert.ToByte(Math.Round(b, 0));
            }
            else if (i < 11 * change)
            {
                double j = i - 10 * change;
                A = 255;
                double r = 25 + j * 26 / change;
                R = Convert.ToByte(Math.Round(r, 0));
                double g = 219 + j * 36 / change;
                G = Convert.ToByte(Math.Round(g, 0));
                double b = 89 + j * 89 / change;
                B = Convert.ToByte(Math.Round(b, 0));
            }
            else if (i < 12 * change)
            {
                double j = i - 11 * change;
                A = 255;
                double r = 51 - j * 51 / change;
                R = Convert.ToByte(Math.Round(r, 0));
                double g = 255 - j * 45 / change;
                G = Convert.ToByte(Math.Round(g, 0));
                double b = 178 + j * 32 / change;
                B = Convert.ToByte(Math.Round(b, 0));
            }
            else if (i < 13 * change)
            {
                double j = i - 12 * change;
                A = 255;
                double r = 0 + j * 15 / change;
                R = Convert.ToByte(Math.Round(r, 0));
                double g = 210 - j * 83 / change;
                G = Convert.ToByte(Math.Round(g, 0));
                double b = 210 - j * 83 / change;
                B = Convert.ToByte(Math.Round(b, 0));
            }
            else if (i < 14 * change)
            {
                double j = i - 13 * change;
                A = 255;
                double r = 15 + j * 6 / change;
                R = Convert.ToByte(Math.Round(r, 0));
                double g = 127 - j * 31 / change;
                G = Convert.ToByte(Math.Round(g, 0));
                double b = 127 + j * 62 / change;
                B = Convert.ToByte(Math.Round(b, 0));
            }
            else if (i < 15 * change)
            {
                double j = i - 14 * change;
                A = 255;
                double r = 21 - j * 10 / change;
                R = Convert.ToByte(Math.Round(r, 0));
                double g = 96 - j * 38 / change;
                G = Convert.ToByte(Math.Round(g, 0));
                double b = 189 - j * 31 / change;
                B = Convert.ToByte(Math.Round(b, 0));
            }
            else if (i < 16 * change)
            {
                double j = i - 15 * change;
                A = 255;
                double r = 11 - j * 11 / change;
                R = Convert.ToByte(Math.Round(r, 0));
                double g = 58 - j * 38 / change;
                G = Convert.ToByte(Math.Round(g, 0));
                double b = 158 - j * 31 / change;
                B = Convert.ToByte(Math.Round(b, 0));
            }
            else if (i < 17 * change)
            {
                double j = i - 16 * change;
                A = 255;
                double r = 0 + j * 65 / change;
                R = Convert.ToByte(Math.Round(r, 0));
                double g = 20 + j * 8 / change;
                G = Convert.ToByte(Math.Round(g, 0));
                double b = 127 + j * 60 / change;
                B = Convert.ToByte(Math.Round(b, 0));
            }
            else if (i < 18 * change)
            {
                double j = i - 17 * change;
                A = 255;
                double r = 65 + j * 84 / change;
                R = Convert.ToByte(Math.Round(r, 0));
                double g = 28 + j * 15 / change;
                G = Convert.ToByte(Math.Round(g, 0));
                double b = 187 - j * 33 / change;
                B = Convert.ToByte(Math.Round(b, 0));
            }
            else if (i < 19 * change)
            {
                double j = i - 18 * change;
                A = 255;
                double r = 149 + j * 52 / change;
                R = Convert.ToByte(Math.Round(r, 0));
                double g = 43 + j * 117 / change;
                G = Convert.ToByte(Math.Round(g, 0));
                double b = 154 + j * 66 / change;
                B = Convert.ToByte(Math.Round(b, 0));
            }
            else
            {
                double j = i - 19 * change;
                A = 255;
                double r = 201 + j * 18 / change;
                R = Convert.ToByte(Math.Round(r, 0));
                double g = 160 + j * 52 / change;
                G = Convert.ToByte(Math.Round(g, 0));
                double b = 220 + j * 35 / change;
                B = Convert.ToByte(Math.Round(b, 0));
            }
            Color colortopaint = Color.FromArgb(A, R, G, B);
            return colortopaint;

        }

        //FUNCTIONS CHARTS

        //public void setCharts()
        //{
        //    Values1 = new ChartValues<double> { 3, 4, 6, 3, 2, 6 };
        //    Values2 = new ChartValues<double> { 5, 3, 5, 7, 3, 9 };
        //    DataContext = this;
        //}


        public SeriesCollection SeriesV { get; set; }
        public List<double> Labels { get; set; }
        public void SetChartV()
        {
            int filaCount = nozzlesim.GetRow(0).Count;
            List<List<double>> listV = new List<List<double>>();
            int i = 0;
            foreach (int pos in posChart)
            {
                if (pos == 1)
                {
                    listV.Add(nozzlesim.GetColumnPar(i, "V"));
                }
                else
                    listV.Add(new List<double>());
                i++;
            }
            SeriesV = new SeriesCollection
            {
                new LineSeries
                {
                    Title = "i = 0",
                    Values =  new ChartValues<double>(listV[0]),
                    PointGeometry = null,
                    Fill=Brushes.Transparent,
                },
                new LineSeries
                {
                    Title = "i = 1",
                    Values =  new ChartValues<double>(listV[1]),
                    PointGeometry = null,
                    Fill=Brushes.Transparent,
                },
                new LineSeries
                {
                    Title = "i = 2",
                    Values =  new ChartValues<double>(listV[2]),
                    PointGeometry = null,
                    Fill=Brushes.Transparent,
                },
                new LineSeries
                {
                    Title = "i = 3",
                    Values =  new ChartValues<double>(listV[3]),
                    PointGeometry = null,
                    Fill=Brushes.Transparent,
                },
                new LineSeries
                {
                    Title = "i = 4",
                    Values =  new ChartValues<double>(listV[4]),
                    PointGeometry = null,
                    Fill=Brushes.Transparent,

                },
                new LineSeries
                {
                    Title = "i = 5",
                    Values =  new ChartValues<double>(listV[5]),
                    PointGeometry = null,
                    Fill=Brushes.Transparent,

                },
                new LineSeries
                {
                    Title = "i = 6",
                    Values =  new ChartValues<double>(listV[6]),
                    PointGeometry = null,
                    Fill=Brushes.Transparent,
                },
                new LineSeries
                {
                    Title = "i = 7",
                    Values =  new ChartValues<double>(listV[7]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 8",
                    Values =  new ChartValues<double>(listV[8]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 9",
                    Values =  new ChartValues<double>(listV[9]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 10",
                    Values =  new ChartValues<double>(listV[10]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 11",
                    Values =  new ChartValues<double>(listV[11]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 12",
                    Values =  new ChartValues<double>(listV[12]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 13",
                    Values =  new ChartValues<double>(listV[13]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 14",
                    Values =  new ChartValues<double>(listV[14]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 15",
                    Values =  new ChartValues<double>(listV[15]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 16",
                    Values =  new ChartValues<double>(listV[16]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 17",
                    Values =  new ChartValues<double>(listV[17]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 18",
                    Values =  new ChartValues<double>(listV[18]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 19",
                    Values =  new ChartValues<double>(listV[19]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 20",
                    Values =  new ChartValues<double>(listV[20]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 21",
                    Values =  new ChartValues<double>(listV[21]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 22",
                    Values =  new ChartValues<double>(listV[22]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 23",
                    Values =  new ChartValues<double>(listV[23]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 24",
                    Values =  new ChartValues<double>(listV[24]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 25",
                    Values =  new ChartValues<double>(listV[25]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 26",
                    Values =  new ChartValues<double>(listV[26]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 27",
                    Values =  new ChartValues<double>(listV[27]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 28",
                    Values =  new ChartValues<double>(listV[28]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 29",
                    Values =  new ChartValues<double>(listV[29]),
                    PointGeometry = null,
                },
                new LineSeries
                {
                    Title = "i = 30",
                    Values =  new ChartValues<double>(listV[30]),
                    PointGeometry = null,
                },
                //new LineSeries
                //{
                //    Title = "Series 2",
                //    Values = new ChartValues<double> { 6, 7, 3, 4, 6 },
                //    PointGeometry = null
                //},
                //new LineSeries
                //{
                //    Title = "Series 3",
                //    Values = new ChartValues<double> { 4, 2, 7, 2, 7 },
                //    PointGeometry = DefaultGeometries.Square,
                //    PointGeometrySize = 15
                //}
            };


            var timeList = nozzlesim.getTimeList();
            Labels = new List<double>(timeList);
            //YFormatter = value => value.ToString("C");

            //modifying the series collection will animate and update the chart

            //SeriesV.Add(new LineSeries
            //{
            //    Title = "Series 4",
            //    Values = new ChartValues<double> { 5, 3, 2, 4 },
            //    LineSmoothness = 0, //0: straight lines, 1: really smooth lines
            //    PointGeometry = Geometry.Parse("m 25 70.36218 20 -28 -20 22 -8 -6 z"),
            //    PointGeometrySize = 50,
            //    PointForeground = Brushes.Gray
            //});

            //modifying any series values will also animate and update the chart
            //SeriesV[3].Values.Add(5d);

            DataContext = this;
        }


        public void fillSelectedList()
        {
            int i = 0;
            while (i < nozzlesim.getN())
            {
                posChart.Add(0);
                i++;
            }
            posChart[0] = 1;
            posChart[1] = 1;
            posChart[2] = 1;
        }


    }
}


namespace Wpf.CartesianChart.PointShapeLine
{
    public partial class PointShapeLineExample : UserControl
    {
        public PointShapeLineExample()
        {

            SeriesCollection = new SeriesCollection
            {
                new LineSeries
                {
                    Title = "Series 1",
                    Values = new ChartValues<double> { 4, 6, 5, 2 ,4 }
                },
                new LineSeries
                {
                    Title = "Series 2",
                    Values = new ChartValues<double> { 6, 7, 3, 4 ,6 },
                    PointGeometry = null
                },
                new LineSeries
                {
                    Title = "Series 3",
                    Values = new ChartValues<double> { 4,2,7,2,7 },
                    PointGeometry = DefaultGeometries.Square,
                    PointGeometrySize = 15
                }
            };

            Labels = new[] { "Jan", "Feb", "Mar", "Apr", "May" };
            YFormatter = value => value.ToString("C");

            //modifying the series collection will animate and update the chart
            SeriesCollection.Add(new LineSeries
            {
                Title = "Series 4",
                Values = new ChartValues<double> { 5, 3, 2, 4 },
                LineSmoothness = 0, //0: straight lines, 1: really smooth lines
                PointGeometry = Geometry.Parse("m 25 70.36218 20 -28 -20 22 -8 -6 z"),
                PointGeometrySize = 50,
                PointForeground = Brushes.Gray
            });

            //modifying any series values will also animate and update the chart
            SeriesCollection[3].Values.Add(5d);

            DataContext = this;
        }

        public SeriesCollection SeriesCollection { get; set; }
        public string[] Labels { get; set; }
        public Func<double, string> YFormatter { get; set; }

    }
}



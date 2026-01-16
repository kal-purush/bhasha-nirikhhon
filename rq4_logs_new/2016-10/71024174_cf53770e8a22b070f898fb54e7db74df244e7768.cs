using System;
using System.Collections.Generic;
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
using TheMathMaze;

namespace TheMathMazeWindow
{
    /// <summary>
    /// MainWindow.xaml 的交互逻辑
    /// </summary>
    public partial class MainWindow : Window
    {

        bool _console_mode = true;
        bool console_mode
        {
            get
            {
                return _console_mode;
            }
            set
            {
                if (value == true)
                {
                    textBox1.IsReadOnly = false;
                    textBox1.Background = new SolidColorBrush(Color.FromArgb(128, 0, 0, 0));
                    console_output("Switched to Console Mode\r\n");
                    buttonadd.IsEnabled = false;
                    buttonsub.IsEnabled = false;
                    buttonmul.IsEnabled = false;
                    buttondiv.IsEnabled = false;
                    buttonaddline.IsEnabled = false;
                    buttonsubline.IsEnabled = false;

                    textBoxInput.IsEnabled = false;
                    textBoxInput.Background = new SolidColorBrush(Color.FromArgb(128, 128, 128, 128));
                }
                else
                {
                    //TODO:别忘了clear
                    //textBox1.Clear();
                    textBox1.IsReadOnly = true;
                    textBox1.Background = new SolidColorBrush(Color.FromArgb(128, 128, 128, 128));
                    console_output("Switched to GUI Mode\r\n");
                    buttonadd.IsEnabled = true;
                    buttonsub.IsEnabled = true;
                    buttonmul.IsEnabled = true;
                    buttondiv.IsEnabled = true;

                    textBoxInput.IsEnabled = true;
                    textBoxInput.Background = new SolidColorBrush(Color.FromArgb(128, 255, 255, 255));
                }
                _console_mode = value;
            }
        }

        BaseEquation.METHOD _GUIMethod = BaseEquation.METHOD.ADD;
        //TODO: setget

        public MainWindow()
        {
            InitializeComponent();
            //buttonadd.IsEnabled = false;
            //buttonsub.IsEnabled = false;
            //buttonmul.IsEnabled = false;
            //buttondiv.IsEnabled = false;
            //buttonaddline.IsEnabled = false;
            //buttonsubline.IsEnabled = false;


            console_mode = true;
            console_output("Initialize finished\r\n");
        }
        private void Window_MouseDown(object sender, MouseButtonEventArgs e)
        {
            if (e.LeftButton == MouseButtonState.Pressed)
            {
                DragMove();
            }
        }
        private void BtnClose_OnClick(object sender, EventArgs e)
        {
            Close();
        }

        private void button_Click(object sender, RoutedEventArgs e)
        {
            //textBox2.Text = "";
            //textBox2.AppendText(textBox1.Text + "\r\n");
            DateTime t1 = DateTime.Now;
            string ans = ConsoleMazeMain.get_result(textBox1.Text);
            int milisec = (int)DateTime.Now.Subtract(t1).TotalMilliseconds;
            int ans_num = ans.Split(new char[1] { '\r' }).Length - 1;
            console_output(ans + "Quest:\r\n" + textBox1.Text + "\r\n" + ans_num.ToString() + " answer(s) was found.\r\n" + "Elapsed Time：" + milisec.ToString() + "ms\r\n");
        }

        private void button_switch_Click(object sender, RoutedEventArgs e)
        {
            console_mode = !console_mode;
        }
        private void console_output(string str)
        {
            textBox2.AppendText(str + "\r\n");
            textBox2.ScrollToEnd();
        }

        private void button_add(object sender, RoutedEventArgs e)
        {

        }

        private void button_sub(object sender, RoutedEventArgs e)
        {

        }

        private void button_mul(object sender, RoutedEventArgs e)
        {

        }

        private void button_div(object sender, RoutedEventArgs e)
        {

        }

        private void button_add_line(object sender, RoutedEventArgs e)
        {

        }

        private void button_sub_line(object sender, RoutedEventArgs e)
        {

        }
    }
}
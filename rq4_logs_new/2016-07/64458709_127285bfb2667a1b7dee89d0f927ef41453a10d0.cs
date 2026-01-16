using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;

namespace epubReader4._0_Dino
{
    /// <summary>
    /// AnnotationToolsWindow.xaml の相互作用ロジック
    /// </summary>
    public partial class AnnotationToolsWindow : Window
    {
        public AnnotationToolsWindow()
        {
            InitializeComponent();
        }

        bool isStraightMode = false; //falseが自由線、trueが直線

        //ツールボックスの移動
        private void Window_MouseLeftButtonDown(object sender, MouseButtonEventArgs e)
        {
            this.DragMove();
        }

        //直線・自由線切り替え
        private void ModeSwitchButton_Click(object sender, RoutedEventArgs e)
        {
            if (!isStraightMode) //直線モードにする
            {
                ((MainWindow)this.Owner).ChangeStrokeMode("straight");
                ModeSwitchButton.Content = "自由線";
                isStraightMode = true;
            }
            else //自由線にする
            {
                ((MainWindow)this.Owner).ChangeStrokeMode("free");
                ModeSwitchButton.Content = "直線";
                isStraightMode = false;

            }
        }

        //色変更
        private void BlackButton_Click(object sender, RoutedEventArgs e)
        {
            ((MainWindow)this.Owner).ChangeColor("0", "0", "0");
        }

        private void WhiteButton_Click(object sender, RoutedEventArgs e)
        {
            ((MainWindow)this.Owner).ChangeColor("255", "255", "255");
        }

        private void RedButton_Click(object sender, RoutedEventArgs e)
        {
            ((MainWindow)this.Owner).ChangeColor("255", "0", "0");
        }

        private void BlueButton_Click(object sender, RoutedEventArgs e)
        {
            ((MainWindow)this.Owner).ChangeColor("0", "0", "255");
        }

        private void GreenButton_Click(object sender, RoutedEventArgs e)
        {
            ((MainWindow)this.Owner).ChangeColor("0", "255", "0");
        }

        private void YellowButton_Click(object sender, RoutedEventArgs e)
        {
            ((MainWindow)this.Owner).ChangeColor("255", "255", "0");
        }

        //太さ変更
        private void Width1Button_Click(object sender, RoutedEventArgs e)
        {
            ((MainWindow)this.Owner).ChangeWidth("1");
        }

        private void Width3Button_Click(object sender, RoutedEventArgs e)
        {
            ((MainWindow)this.Owner).ChangeWidth("3");
        }

        private void Width20Button_Click(object sender, RoutedEventArgs e)
        {
            ((MainWindow)this.Owner).ChangeWidth("20");
        }

        //一つ戻る
        private void UndoButton_Click(object sender, RoutedEventArgs e)
        {
            ((MainWindow)this.Owner).Undo();
        }

        //全消去
        private void ClearButton_Click(object sender, RoutedEventArgs e)
        {
            ((MainWindow)this.Owner).ClearStroke();
        }

        //閉じる
        private void CloseButton_Click(object sender, RoutedEventArgs e)
        {
            //一本でもストロークがあればキャプチャする
            int numberOfPath = ((MainWindow)this.Owner).getNumberOfPath();
            if (numberOfPath > 0)
            {
                ((MainWindow)this.Owner).BrowserCapture();
            }
            ((MainWindow)this.Owner).CloseCanvas();
            this.Close();
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
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

namespace TestWPF3
{
    /// <summary>
    /// Логика взаимодействия для MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();
            //DataContext = new MainWindowViewModel();
        }
        private CancellationTokenSource _ProcessCancellation;
        private async void OnStartButtonClick(object Sender, RoutedEventArgs E)
        {
            if (!(Sender is Button button)) return;
            button.IsEnabled = false;

            var cancellation = new CancellationTokenSource();
            //Interlocked.Exchange(ref _ProcessCancellation, cancellation)?.Cancel(); // более правильный подход для многопоточного приложения
            _ProcessCancellation?.Cancel();
            _ProcessCancellation = cancellation;

            const string message = "Hello World!";

            ResultRun.Text = "Начался расчёт";

            IProgress<int> progress = new Progress<int>(p => _Progress.Value = p);
            try
            {
                var result = await GetMessageLengthAsync(message, 30, progress, cancellation.Token).ConfigureAwait(true);

                progress.Report(0);

                ResultRun.Text = result.ToString();
            }
            catch (OperationCanceledException e)
            {
                ResultRun.Text = "Выполнен сброс";
                progress.Report(0);
            }
            button.IsEnabled = true;
        }

        private void OnCancelButtonClick(object Sender, RoutedEventArgs E)
        {
            _ProcessCancellation?.Cancel();
        }

        private async Task<int> GetMessageLengthAsync(string Message, int Timeout = 30, IProgress<int> Progress = null, CancellationToken Cancel = default)
        {
            return await Task.Run(() => GetLengthAsync(Message, Timeout, Progress, Cancel), Cancel).ConfigureAwait(false);
        }

        private int _StartCount;
        private int GetMessageLength(string Message, int Timeout = 30, IProgress<int> Progress = null, CancellationToken Cancel = default)
        {
            for (var i = 0; i < 100; i++)
            {
                Thread.Sleep(Timeout);
                Progress?.Report(i);

                Cancel.ThrowIfCancellationRequested();
            }

            return Message.Length + _StartCount++;
        }

        private async Task<int> GetLengthAsync(string Message, int Timeout = 30, IProgress<int> Progress = null, CancellationToken Cancel = default)
        {
            for (var i = 0; i < 100; i++)
            {
                await Task.Delay(Timeout, Cancel);

                //if (Cancel.IsCancellationRequested)
                //    Progress?.Report(0);
                //else
                Progress?.Report(i);

                Cancel.ThrowIfCancellationRequested();
            }

            return Message.Length + _StartCount++;
        }
    }
}
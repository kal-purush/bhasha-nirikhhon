using System.Speech.Synthesis;
using System.Windows;

namespace WPFDemo
{
    /// <summary>
    /// Interaction logic for SpeechWin.xaml
    /// </summary>
    public partial class SpeechWin : Window
    {
        public SpeechWin()
        {
            InitializeComponent();
        }

        private void ExitBtn_Click(object sender, RoutedEventArgs e)
        {
            this.Close();
        }
        private void ExitBtn_Click_1(object sender, RoutedEventArgs e)
        {
            this.Close();
        }
        
        /// <summary>
        /// For the speech synthesis I had to import NuGet package System.Speech (7.0.0)
        /// it then takes a string and converts it into a PromptBuilder which it can then
        /// synthesise into speech.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void EnterBtn_Click(object sender, RoutedEventArgs e)
        {
            string Name = NameBox.Text.ToString();
            PromptBuilder builder = new PromptBuilder();
            builder.AppendText("Hello " + Name + " I hope you are having a good day.");
            SpeechSynthesizer speechSynthesizer = new SpeechSynthesizer();
            speechSynthesizer.Speak(builder);
        }
    }
}
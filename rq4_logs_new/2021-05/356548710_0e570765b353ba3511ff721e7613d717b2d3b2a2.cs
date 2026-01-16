using System.Windows;

namespace Spicy
{
    public partial class AddingMelodyForm : Window
    {
        MainForm owner;

        public AddingMelodyForm(Window owner)
        {
            InitializeComponent();
            InitializeOtherComponent(owner);
        }

        void InitializeOtherComponent(Window window)
        {
            Owner = window;
            owner = window as MainForm;
            ListBoxFunctions.LoadFileNamesFromFolderToList(ListBoxOfMelodies, "music", "mp3");
            ListBoxFunctions.RemoveNamesOfFirstListBoxFromSecondListBox(owner.ListBoxOfMelodies, ListBoxOfMelodies);
            ListBoxFunctions.SortAscending(ListBoxOfMelodies);
        }

        void AddMelodyButton_Click(object sender, RoutedEventArgs e)
        {
            if (MelodyIsReadyToAdding())
            {
                ListBoxFunctions.AddSuitableObjectToSuitableListBox(this);
                Close();
            }
        }

        bool MelodyIsReadyToAdding()
        {
            bool isReady = true;
            if (ListBoxOfMelodies.SelectedItem == null)
            {
                MessageBox.Show("Пожалуйста, выберите мелодию", "Внимание", MessageBoxButton.OK, MessageBoxImage.Warning);
                isReady = false;
            }
            return isReady;
        }
    }
}
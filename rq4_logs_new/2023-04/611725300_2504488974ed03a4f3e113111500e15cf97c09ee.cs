using Project;
using System.Collections.ObjectModel;

namespace UnitTests
{
    [TestClass]
    public class AddParticipantsPage_UnitTests
    {
        [WpfTestMethod]
        public void UserStory3_Test_1()
        {
            // Arrange
            AddParticipantsPage Page = new AddParticipantsPage();
            Page.PersonNameInput = new System.Windows.Controls.TextBox();
            int namesToAdd = 20;
            int namesToRemove = 15;
            int NamesLeft = namesToAdd - namesToRemove;


            // Act
            for (int i = 0; i < namesToAdd; i++)
            {
                Page.PersonNameInput.Text = $"name {i}";
                Page.AddPersonButton(new object(), new System.Windows.RoutedEventArgs());
            }

            for (int i = 0; i < namesToRemove; i++)
            {
                Page.RemovePersonButton(new object(), new System.Windows.RoutedEventArgs());
            }

            // Assert

            Assert.AreEqual(NamesLeft, Page.ScrollViewItems.Count);
        }
    }
}
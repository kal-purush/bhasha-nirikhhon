using NUnit.Framework;
using DFC.App.MatchSkills.ViewComponents.SkillsList;
using FluentAssertions;

namespace DFC.App.MatchSkills.Test.Unit.ViewComponents
{
    [TestFixture]
    public class SkillsListViewModelUnitTests
    {
        public void When_Created_Then_RequiredFieldsMustBePopulated()
        {
            // Arrange
            var itemIdPrefix = "itemIdPrefix";
            var listItemType = SkillsListViewModel.ListItemType.Radio;

            // Act
            var vm = new SkillsListViewModel(itemIdPrefix, listItemType);

            // Assert
            vm.ItemIdPrefix.Should().Be(itemIdPrefix);
            vm.ListType.Should().Be(listItemType);
        }

        public void When_Created_Then_SkillsShouldBeEmpty()
        {
            // Arrange
            var itemIdPrefix = "itemIdPrefix";
            var listItemType = SkillsListViewModel.ListItemType.Radio;

            // Act
            var vm = new SkillsListViewModel(itemIdPrefix, listItemType);

            // Assert
            vm.Skills.Should().NotBeNull();
            vm.Skills.Should().BeEmpty();
        }

        public void When_SettingHTML_Then_HTMLShouldBeSet()
        {
            // Arrange
            var itemIdPrefix = "itemIdPrefix";
            var listItemType = SkillsListViewModel.ListItemType.Radio;
            var vm = new SkillsListViewModel(itemIdPrefix, listItemType);

            // Act
            vm.BeginSkillsListHTML = "<ul>";
            vm.EndSkillsListHTML = "</ul>";
            vm.NoSkillsHTML = "No skills";

            // Assert
            vm.BeginSkillsListHTML.Should().Be("<ul>");
            vm.EndSkillsListHTML.Should().Be("</ul>");
            vm.NoSkillsHTML.Should().Be("No skills");
        }
    }
}
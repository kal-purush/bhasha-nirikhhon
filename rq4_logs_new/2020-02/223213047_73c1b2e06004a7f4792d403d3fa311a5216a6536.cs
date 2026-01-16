using DFC.App.MatchSkills.Models;
using Dfc.ProviderPortal.Packages;

namespace DFC.App.MatchSkills.ViewComponents.SkillsList
{
    public class SkillsListViewModel
    {
        public enum ListItemType
        {
            None,
            Bullet,
            Radio,
            Checkbox
        }

        public ListItemType ListType { get; }

        public SkillSet Skills { get; set; }

        public string NoSkillsHTML { get; set; }

        public string BeginSkillsListHTML { get; set; }
        public string EndSkillsListHTML { get; set; }

        public string ItemIdPrefix { get; }

        public SkillsListViewModel(string itemIdPrefix, ListItemType listType)
        {
            Throw.IfNullOrWhiteSpace(itemIdPrefix,nameof(itemIdPrefix));

            ItemIdPrefix = itemIdPrefix;
            ListType = listType;
            Skills = new SkillSet();
        }
    }
}
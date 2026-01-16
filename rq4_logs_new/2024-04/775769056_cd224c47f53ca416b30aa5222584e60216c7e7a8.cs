using LagDaemon.YAMUD.Model.Characters;
using LagDaemon.YAMUD.Model.Utilities;

namespace LagDaemon.YAMUD.Automation
{
    public interface IPlugin
    {
        void Initialize();
    }

    public interface IQuestPlugin : IPlugin
    {
        void OnQuestProgress(Character character, QuestProgress progress);
        // Other methods related to quests
    }

    public interface ICharacterBehaviorPlugin : IPlugin
    {
        void OnCharacterAction(Character character, ActionType action);
        // Other methods related to character behavior
    }


}
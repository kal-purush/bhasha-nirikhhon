using System.Collections.Generic;
using UnityEngine;

namespace GB_Platformer
{
    internal sealed class CommonQuestStory : QuestStoryBase
    {
        public CommonQuestStory(List<IQuest> questCollections) : base(questCollections)
        {
        }

        protected override void ResetMethod(int index = 0)
        {
            if (index < 0 || index >= _questsCollection.Count)
            {
                return;
            }

            IQuest nextQuest = _questsCollection[index];
            if (nextQuest.IsCompleted)
            {
                OnQuestCompleted(nextQuest);
            }
            else
            {
                _questsCollection[index].Reset();
            }
        }

        protected override void OnQuestCompleted(IQuest quest)
        {
            int index = _questsCollection.IndexOf(quest);
            if (!IsDone)
            {
                ResetMethod(++index);
                return;
            }
            Debug.Log("Complete");
        }
    }
}
using BrainyStories.Objects;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;

namespace BrainyStories
{
    public class ThinkAndDoFactory
    {
        public ObservableCollection<ThinkAndDo> generateThinkAndDos()
        {
            ObservableCollection<ThinkAndDo> ThinkAndDoListTemp = new ObservableCollection<ThinkAndDo>();
            ThinkAndDoListTemp.Add(new ThinkAndDo {
                ThinkAndDoName = "Test",
                AssociatedStory = "S1",
                ThinkAndDoAudioClip = "I7_NBTY_IG.mp3",
                Length = new TimeSpan(0, 2, 2)
            });
            ThinkAndDoListTemp.Add(new ThinkAndDo
            {
                ThinkAndDoName = "Test2",
                AssociatedStory = "S2",
                ThinkAndDoAudioClip = "I8_IAPOD_IG.mp3",
                Length = new TimeSpan(0, 2, 51)
            });

            return ThinkAndDoListTemp;
        }
    }
}
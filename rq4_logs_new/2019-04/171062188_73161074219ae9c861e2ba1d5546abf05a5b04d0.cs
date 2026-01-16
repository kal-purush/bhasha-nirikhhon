using System;
using System.Collections.Generic;
using System.Text;
using WordTutor.Core.Actions;
using WordTutor.Core.Redux;

namespace WordTutor.Core
{
    public class ScreenReducer : IReduxReducer<Screen>
    {
        public Screen Reduce(IReduxMessage message, Screen screen)
        {
            switch (screen)
            {
                case AddVocabularyWordScreen add:
                    return ReduceAddVocabularyWord(message, add);

                default: return screen;
            }
        }

        private Screen ReduceAddVocabularyWord(IReduxMessage message, AddVocabularyWordScreen screen)
        {
            switch (message)
            {
                case ModifySpellingMessage msg:
                    return screen.WithSpelling(msg.Spelling);

                case ModifyPhraseMessage msg:
                    return screen.WithPhrase(msg.Phrase);

                case ModifyPronunciationMessage msg:
                    return screen.WithPronunciation(msg.Pronunciation);

                default:
                    return screen;
            }
        }
    }
}
using System;

namespace GB_Platformer
{
    internal sealed class Quest : IQuest
    {
        private readonly SpriteAnimator _spriteAnimator;
        private readonly QuestObjectView _questObjectView;
        private readonly IQuestLogic _questLogic;
        private bool _isActive;

        public bool IsCompleted { get; private set; }
        public event Action<IQuest> Completed;

        public Quest(QuestObjectView questObjectView, IQuestLogic questLogic, SpriteAnimator spriteAnimator)
        {
            _questObjectView = questObjectView;
            _questLogic = questLogic;
            _spriteAnimator = spriteAnimator;
        }

        public void Reset()
        {
            if (_isActive)
            {
                return;
            }
            _isActive = true;
            IsCompleted = false;
            _questObjectView.OnObjectContact += OnContact;
            _questObjectView.ProcessActivate(_spriteAnimator);

        }

        public void Dispose()
        {
            _questObjectView.OnObjectContact -= OnContact;
        }

        private void Complete()
        {
            if (!_isActive)
            {
                return;
            }
            _isActive = false;
            IsCompleted = true;
            _questObjectView.OnObjectContact -= OnContact;
            _questObjectView.ProcessComplete();
            OnCompleted();
        }

        private void OnCompleted()
        {
            Completed?.Invoke(this);
        }

        private void OnContact(LevelObjectView levelObjectView)
        {
            bool completed = _questLogic.TryComplete(levelObjectView.gameObject);
            if (completed)
            {
                Complete();
            }
        }
    }
}
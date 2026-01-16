namespace GB_Platformer
{
    internal sealed class AbilitiesController :IInitialization, IDeinitialization
    {
        private readonly PlayerInfo _playerInfo;
        private readonly QuestObjectView[] _questObjects;

        public AbilitiesController(PlayerInfo playerInfo, QuestObjectView[] questObjects)
        {
            _playerInfo = playerInfo;
            _questObjects = questObjects;
        }

        public void Initialization()
        {
            for (int i = 0; i < _questObjects.Length; i++)
            {
                _questObjects[i].QuestItem += qq;
            }
        }

        public void Deinitialization()
        {
            for (int i = 0; i < _questObjects.Length; i++)
            {
                _questObjects[i].QuestItem -= qq;
            }
        }

        private void qq(int questItemId)
        {
            switch (questItemId)
            {
                case 1:
                    _playerInfo.Abilities.Weapon = true;
                    break;
                case 2:
                    _playerInfo.Abilities.AbleJump = true;
                    break;
                case 3:
                    _playerInfo.Abilities.OnceIncreaseMaxHeath = true;
                    _playerInfo.PlayerView.Health.MaxHealth += Constants.Variables.POTION_HEATH_POINT;
                    _playerInfo.PlayerView.Health.CurrentHealth += Constants.Variables.POTION_HEATH_POINT;
                    _playerInfo.PlayerView.ChangeHeath();
                    break;
                case 4:
                    _playerInfo.Abilities.ExitKey = true;
                    break;
                default:
                    break;
            }
        }
    } 
}
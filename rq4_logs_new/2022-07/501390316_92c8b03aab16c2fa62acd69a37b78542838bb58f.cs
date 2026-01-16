using UnityEngine;
using UnityEngine.SceneManagement;

namespace GB_Platformer
{
    internal sealed class ExitZone : IInitialization, IDeinitialization
    {
        private readonly SpriteAnimator _spriteAnimator;
        private readonly DoorTriggerView _view;
        private readonly LevelObjectTrigger _exit;
        private readonly PlayerInfo _playerInfo;

        public ExitZone(DoorTriggerView view, LevelObjectTrigger exit, PlayerInfo playerInfo, SpriteAnimator spriteAnimator)
        {
            _view = view;
            _exit = exit;
            _playerInfo = playerInfo;
            _spriteAnimator = spriteAnimator;
        }

        public void Initialization()
        {
            _spriteAnimator.StartAnimation(_view.SpriteRenderer, Track.Exit_Default, true, Constants.Variables.ANIMATIONS_SPEED);
            _view.TriggerEnter += OnContact;
            _exit.BoxCollider2D.enabled = false;
            _exit.TriggerEnter += Exit;
        }

        public void Deinitialization()
        {
            _view.TriggerEnter -= OnContact;
        }

        private void OnContact(GameObject gameObject)
        {
            if (!gameObject.GetComponent<PlayerView>())
            {
                return;
            }
            if (!_playerInfo.Abilities.ExitKey)
            {
                return;
            }

            _spriteAnimator.StartAnimation(_view.SpriteRenderer, Track.Exit_Active, false, Constants.Variables.ANIMATIONS_SPEED);
            _view.BoxCollider.enabled = false;
            _exit.BoxCollider2D.enabled = true;
        }

        private void Exit(GameObject obj)
        {
            int indexScene = SceneManager.GetActiveScene().buildIndex;
            if (indexScene < SceneManager.sceneCountInBuildSettings - 1)
            {
                SceneManager.LoadScene(++indexScene);
            }
        }
    }
}
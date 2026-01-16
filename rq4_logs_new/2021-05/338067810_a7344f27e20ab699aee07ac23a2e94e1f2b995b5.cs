using UnityEngine;
using UnityEngine.UI;
using Assets.Scripts.Overlay.GameInfo;

namespace Assets.Scripts.Overlay.GameStates
{
    public class GameOver : MonoBehaviour
    {
        [SerializeField] private Text _pointsTextField;

        public void Awake()
        {
            gameObject.SetActive(false);
        }

        public void MainMenu() { SceneLoader.MainMenu(); }

        public void Replay() { SceneLoader.ReloadScene(); }

        public void Show()
        {
            _pointsTextField.text = "Final score: " + PointsOverlay.Current.Points;

            gameObject.SetActive(true);
        }
    }
}
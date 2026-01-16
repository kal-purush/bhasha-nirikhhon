using UnityEngine;
using UnityEngine.UI;
using TMPro;

namespace Vulpes.Menus
{
    [AddComponentMenu("Vulpes/Menus/Menu Loading")]
    public sealed class MenuLoading : MenuScreen
    {
        // All values on this screen are optional in order to allow it to be easily customised on a per-project basis.

        // Progress
        [SerializeField] private TextMeshProUGUI progressPercentageText = default;
        [SerializeField] private Slider progressBarSlider = default;

        // Spinner
        [SerializeField] private RectTransform spinnerRectTransform = default;
        [SerializeField] private float spinnerSpeed = 360.0f;
        
        // Map Name
        [SerializeField] private TextMeshProUGUI mapNameText = default;

        // Tips
        [SerializeField] private TextMeshProUGUI tipTitleText = default;
        [SerializeField] private TextMeshProUGUI tipSubtitleText = default;
        [SerializeField] private TextMeshProUGUI tipBodyText = default;
        [SerializeField] private RawImage tipImage = default;
        [SerializeField] private Button tipPreviousButton = default;
        [SerializeField] private Button tipNextButton = default;

        [SerializeField] private MenuLoadingTip[] loadingTips = default;
        private int tipIndex;

        public override void OnInitialize()
        {
            base.OnInitialize();
            SetTip(0);
        }

        public override void OnWillAppear()
        {
            base.OnWillAppear();
            SetProgress(0.0f);
            if (tipPreviousButton != null)
            {
                tipPreviousButton.onClick.AddListener(OnTipPreviousButtonPressed);
            }
            if (tipNextButton != null)
            {
                tipNextButton.onClick.AddListener(OnTipNextButtonPressed);
            }
        }

        public override void OnWillDisappear()
        {
            base.OnWillDisappear();
            if (tipPreviousButton != null)
            {
                tipPreviousButton.onClick.RemoveAllListeners();
            }
            if (tipNextButton != null)
            {
                tipNextButton.onClick.RemoveAllListeners();
            }
        }

        public override void OnActive()
        {
            base.OnActive();
            if (spinnerRectTransform != null)
            {
                spinnerRectTransform.Rotate(Vector3.forward, Time.deltaTime * -spinnerSpeed, Space.Self);
            }
        }

        /// <summary>
        /// Sets the progress value for the percentage text and progress slider if they are assigned.
        /// </summary>
        public void SetProgress(float progress)
        {
            if (progressPercentageText != null)
            {
                int progressPercentage = Mathf.RoundToInt(progress * 100.0f);
                progressPercentageText.text = $"{progressPercentage.ToString()}%";
            }
            if (progressBarSlider != null)
            {
                progressBarSlider.value = progress;
            }
        }

        /// <summary>
        /// Sets the map name text if it is assigned.
        /// </summary>
        public void SetMapName(string mapName)
        {
            if (mapNameText != null)
            {
                mapNameText.text = mapName;
            }
        }

        /// <summary>
        /// Sets the various tip text and image fields if they are assigned. 
        /// </summary>
        public void SetTip(string title, string subtitle, string body, Texture texture = null)
        {
            if (tipTitleText != null)
            {
                tipTitleText.text = title;
            }
            if (tipSubtitleText != null)
            {
                tipSubtitleText.text = subtitle;
            }
            if (tipBodyText != null)
            {
                tipBodyText.text = body;
            }
            if (tipImage != null)
            {
                tipImage.texture = texture;
            }
        }

        private void SetTip(int index)
        {
            if (loadingTips.Length > 0)
            {
                tipIndex = index;
                SetTip(loadingTips[tipIndex].title, loadingTips[tipIndex].subtitle, loadingTips[tipIndex].body, loadingTips[tipIndex].texture);
            }
        }

        private void OnTipPreviousButtonPressed()
        {
            // TODO
        }

        private void OnTipNextButtonPressed()
        {
            // TODO
        }
    }
}
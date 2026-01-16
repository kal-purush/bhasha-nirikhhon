using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class MusicSectionController : MonoBehaviour {
    public Component storageScript, storyController;
    public AudioSource musicPlayer;
    public Sprite[] backgroundImages;
    public Canvas background, textBox;
    public Button nextButton, menuButton, closeMenuButton;
    public bool isVertical = false;

    LyricStorageInterface storage;

    Canvas bg;
    RectTransform bgRect;
    Button _nextButton, _menuButton, _closeMenuButton;
    Text _text;
    float bgWidth;
    float bgHeight;
    float currentTime = 0;
    int nextLyric = 0;

    void Awake () {
        storage = storageScript.GetComponent<LyricStorageInterface>();
        bg = background.GetComponent<Canvas>();
        bgRect = background.GetComponent<RectTransform>();
        _nextButton = nextButton.GetComponent<Button>();
        _menuButton = menuButton.GetComponent<Button>();
        _closeMenuButton = closeMenuButton.GetComponent<Button>();
        _text = textBox.gameObject.GetComponentInChildren<Text>();
	}

	void Start () {
        _menuButton.onClick.AddListener(OnClickMenu);
        _closeMenuButton.onClick.AddListener(OnCloseMenu);

        _nextButton.onClick.AddListener(OnClickNext);
        _nextButton.gameObject.SetActive(false);
	}

    void Update () {
        // move when music is playing

        if (musicPlayer.isPlaying && currentTime <= storage.endTime) {
            currentTime += Time.deltaTime;

            if (isVertical) {

            } else {
                if (bgWidth > (Mathf.Abs(bgRect.anchoredPosition.x))) {
                    float targetX = (Time.deltaTime / storage.endTime) * bgWidth;
                    bgRect.Translate(new Vector3(-targetX, 0, 0));
                } else {
                    bgRect.anchoredPosition = new Vector2(-bgWidth, 0);
                    nextButton.gameObject.SetActive(true);
                }
            }

            // lyrics updater
            if (nextLyric < storage.lyricModels.Count && currentTime >= storage.lyricModels[nextLyric].startTime) {
                int index = storage.lyricModels[nextLyric].lyricIndex;
                _text.text = storage.lyrics[index];
                nextLyric += 1;
            }
        }
    }

    public void Setup () {
        currentTime = 0;
        nextLyric = 0;

        SetupImages();

        if (PlayerPrefs.GetInt("music", 1) == 0) {
            musicPlayer.mute = true;
        } else {
            musicPlayer.mute = false;
        }

        musicPlayer.Play();
    }

    public void SetupImages () {
        Image lastImage = null;

        for (int i = 0; i < backgroundImages.Length; i++) {
            Sprite currentSprite = backgroundImages[i];

            GameObject NewObj = new GameObject();
            Image NewImage = NewObj.AddComponent<Image>();

            if (i + 1 == backgroundImages.Length) {
                lastImage = NewImage;
            }

            NewImage.sprite = currentSprite;
            RectTransform rect = NewObj.GetComponent<RectTransform>();
            RectTransform parent = bg.GetComponent<RectTransform>();

            rect.SetParent(bg.transform); // Assign the newly created Image GameObject as a Child of the Parent Panel.

            // Set image sizing and anchors
            rect.transform.SetParent(parent);
            rect.localScale = new Vector3(1, 1, 1);
            rect.anchorMin = new Vector2(0, 0);
            rect.anchorMax = new Vector2(1, 1);
            rect.pivot = new Vector2(0.5f, 0.5f);
            rect.anchoredPosition = new Vector2(0, 0);
            rect.sizeDelta = Vector2.zero;

            if (isVertical) {
                rect.Translate(new Vector3(0, i * Screen.height, 0));
            } else {
                rect.Translate(new Vector3(i * Screen.width, 0, 0));
            }

            NewObj.SetActive(true); //Activate the GameObject
        }

        bgWidth = lastImage.GetComponent<RectTransform>().anchoredPosition.x;
        bgHeight = lastImage.GetComponent<RectTransform>().anchoredPosition.y;
    }

    public void OnClickMenu () {
        musicPlayer.Pause();
    }

    public void OnCloseMenu () {
        musicPlayer.UnPause();
    }

    public void OnClickNext () {
        musicPlayer.Stop();
        this.gameObject.SetActive(false);
        storyController.SendMessage("OnChapterMusicFinish");
    }
}